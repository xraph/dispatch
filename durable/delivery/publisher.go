// Package delivery publishes accepted outbox intents independently of workflow workers.
package delivery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/telemetry"
)

// Sink accepts an immutable envelope durably. Identical retries, including after
// an unknown outcome, must return the same receipt. Conflicting content must fail.
// A transport acknowledgement alone does not satisfy this contract.
type Sink interface {
	Accept(context.Context, durable.Delivery) (durable.SinkReceipt, error)
}

// ErrConfirmedConflict is returned only after a sink confirms conflicting
// durable content for this source identity. Unknown outcomes remain retryable.
var ErrConfirmedConflict = errors.New("dispatch: confirmed sink conflict")

var ErrIncompleteShutdown = errors.New("dispatch: delivery shutdown incomplete")

// Config bounds work separately for each destination. Concurrency is the maximum
// number of calls, including calls whose sink ignores context cancellation.
type Config struct {
	InstallationID string
	Owner          string
	Concurrency    int
	PollInterval   time.Duration
	CallTimeout    time.Duration
	StoreTimeout   time.Duration
	LeaseDuration  time.Duration
	RetryMin       time.Duration
	RetryMax       time.Duration
}

func (c Config) defaults() Config {
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
	if c.PollInterval == 0 {
		c.PollInterval = 100 * time.Millisecond
	}
	if c.CallTimeout == 0 {
		c.CallTimeout = 5 * time.Second
	}
	if c.StoreTimeout == 0 {
		c.StoreTimeout = 5 * time.Second
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.RetryMin == 0 {
		c.RetryMin = time.Second
	}
	if c.RetryMax == 0 {
		c.RetryMax = time.Minute
	}
	return c
}

type Status struct {
	Blocked          int64
	Pending          int64
	OldestAcceptedAt time.Time
	InFlight         int
	ErrorCategory    string
}
type destinationState struct {
	inFlight int
	category string
}

type Publisher struct {
	store       durable.OutboxStore
	config      Config
	sinks       map[durable.Destination]Sink
	mu          sync.Mutex
	states      map[durable.Destination]destinationState
	started     bool
	cancel      context.CancelFunc
	done        chan struct{}
	draining    atomic.Bool
	stopGate    chan struct{}
	interrupted bool
	tel         *telemetry.Telemetry
}

// Option configures a Publisher beyond its Config.
type Option func(*Publisher)

// WithTelemetry sets where the publisher records its spans and metrics: one
// root "deliver outbox event" span per claimed delivery, and the delivery
// duration and lag histograms. The Forge extension passes the engine's own.
func WithTelemetry(t *telemetry.Telemetry) Option {
	return func(p *Publisher) { p.tel = t }
}

func New(store durable.OutboxStore, config Config, sinks map[durable.Destination]Sink, opts ...Option) (*Publisher, error) {
	c := config.defaults()
	if store == nil || !durable.DeliveryIdentifier(c.Owner) || !durable.DeliveryIdentifier(c.InstallationID) || c.Concurrency < 1 || c.Concurrency > durable.MaxDeliveryBatch || c.PollInterval <= 0 || c.CallTimeout <= 0 || c.CallTimeout >= 5*time.Minute || c.StoreTimeout <= 0 || c.StoreTimeout >= 5*time.Minute || c.LeaseDuration < time.Millisecond || c.LeaseDuration <= c.CallTimeout+c.StoreTimeout || c.LeaseDuration > 5*time.Minute || c.RetryMin <= 0 || c.RetryMax < c.RetryMin || c.RetryMax > 24*time.Hour || len(sinks) == 0 {
		return nil, durable.ErrInvalid
	}
	if compatible, ok := store.(durable.DeliveryCompatibilityStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), c.StoreTimeout)
		err := compatible.CheckDeliveryCompatibility(ctx, durable.DeliveryPublisherProtocol)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("dispatch: publisher compatibility: %w", err)
		}
	}
	p := &Publisher{store: store, config: c, sinks: make(map[durable.Destination]Sink), states: make(map[durable.Destination]destinationState), done: make(chan struct{}), stopGate: make(chan struct{}, 1)}
	for destination, sink := range sinks {
		if (durable.DeliveryScope{InstallationID: c.InstallationID, Destination: destination}).Validate() != nil || sink == nil {
			return nil, durable.ErrInvalid
		}
		p.sinks[destination] = sink
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Start owns its lifetime independently of the caller's startup context.
func (p *Publisher) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return errors.New("dispatch: publisher already started")
	}
	p.started = true
	p.startWorkers()
	return nil
}

// startWorkers runs only with p.mu held and after prior workers have exited.
func (p *Publisher) startWorkers() {
	p.done = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	var wg sync.WaitGroup
	for destination, sink := range p.sinks {
		for i := 0; i < p.config.Concurrency; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); p.run(ctx, destination, sink) }()
		}
	}
	go func() { wg.Wait(); close(p.done) }()
}

// Stop drains pending work until the caller's deadline. A canceled or hung sink
// never gets acknowledged and never causes replacement goroutines. Go cannot
// forcibly terminate sink code; incomplete shutdown is reported explicitly.
func (p *Publisher) Stop(ctx context.Context) error {
	select {
	case p.stopGate <- struct{}{}:
		defer func() { <-p.stopGate }()
	case <-ctx.Done():
		return errors.Join(ErrIncompleteShutdown, ctx.Err())
	}
	p.mu.Lock()
	started := p.started
	p.mu.Unlock()
	if !started {
		return nil
	}
	if p.interrupted {
		select {
		case <-p.done:
		case <-ctx.Done():
			return errors.Join(ErrIncompleteShutdown, ctx.Err())
		}
		p.mu.Lock()
		p.startWorkers()
		p.mu.Unlock()
		p.interrupted = false
	}
	p.draining.Store(true)
	select {
	case <-p.done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		p.interrupted = true
		return errors.Join(ErrIncompleteShutdown, ctx.Err())
	}
}

func (p *Publisher) Status(ctx context.Context, destination durable.Destination) (Status, error) {
	ctx, cancel := context.WithTimeout(ctx, p.config.StoreTimeout)
	defer cancel()
	status, err := p.store.DeliveryStatus(ctx, durable.DeliveryStatusRequest{DeliveryScope: p.scope(destination), Limit: 1})
	if err != nil {
		return Status{}, err
	}
	p.mu.Lock()
	s := p.states[destination]
	p.mu.Unlock()
	return Status{Blocked: status.Blocked, Pending: status.Pending, OldestAcceptedAt: status.OldestAcceptedAt, InFlight: s.inFlight, ErrorCategory: s.category}, nil
}
func (p *Publisher) scope(d durable.Destination) durable.DeliveryScope {
	return durable.DeliveryScope{InstallationID: p.config.InstallationID, Destination: d}
}
func (p *Publisher) state(d durable.Destination, delta int, category string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.states[d]
	s.inFlight += delta
	s.category = category
	p.states[d] = s
}
func (p *Publisher) run(ctx context.Context, d durable.Destination, sink Sink) {
	for ctx.Err() == nil {
		storeCtx, cancel := context.WithTimeout(ctx, p.config.StoreTimeout)
		records, err := p.store.ClaimDeliveries(storeCtx, durable.DeliveryClaim{DeliveryScope: p.scope(d), Owner: p.config.Owner, Limit: 1, LeaseDuration: p.config.LeaseDuration})
		cancel()
		if ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			p.state(d, 0, "unavailable")
		case len(records) > 0:
			p.publish(ctx, d, sink, records[0])
			continue
		case p.draining.Load():
			status, statusErr := p.Status(ctx, d)
			if statusErr == nil && status.Pending == 0 {
				return
			}
		}
		timer := time.NewTimer(p.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (p *Publisher) publish(ctx context.Context, d durable.Destination, sink Sink, record durable.DeliveryRecord) {
	// One root span per claimed delivery. ctx descends from
	// context.Background (startWorkers), and an empty claim never gets
	// here, so idle polling starts no traces.
	ctx, span := p.tel.Start(ctx, "deliver outbox event",
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("dispatch.outbox.destination", string(d)),
			attribute.String("dispatch.outbox.delivery_id", record.Delivery.ID),
			attribute.Int64("dispatch.outbox.attempt", record.Attempts),
		),
	)
	started := time.Now()
	// Every path that returns early does so because Stop cancelled ctx, so
	// that is the starting result. The success path and the failure
	// categories overwrite it.
	result := telemetry.OutboxCancelled
	defer func() {
		span.SetAttributes(attribute.String("dispatch.outbox.result", result))
		if result != telemetry.OutboxAcknowledged && result != telemetry.OutboxCancelled {
			telemetry.Fail(span, "delivery "+result, "")
		}
		p.tel.Outbox().Delivery(string(d), result, time.Since(started))
		span.End()
	}()

	category := ""
	envelope := record.Delivery
	if envelope.Destination != d || envelope.InstallationID != p.config.InstallationID || envelope.Verify() != nil {
		category = "rejected"
	} else {
		callCtx, cancel := context.WithTimeout(ctx, p.config.CallTimeout)
		p.state(d, 1, "")
		timeoutNotice := context.AfterFunc(callCtx, func() {
			if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
				p.state(d, 0, "timeout")
			}
		})
		receipt, err := accept(callCtx, sink, envelope)
		callErr := callCtx.Err()
		timeoutNotice()
		cancel()
		p.state(d, -1, "")
		if ctx.Err() != nil {
			return
		}
		switch {
		case callErr != nil:
			category = "timeout"
		case errors.Is(err, ErrConfirmedConflict):
			category = "conflict"
		case err != nil:
			category = "unavailable"
		case receipt.Verify(envelope) != nil:
			category = "invalid_receipt"
		default:
			ackCtx, ackCancel := context.WithTimeout(ctx, p.config.StoreTimeout)
			ackErr := p.store.AcknowledgeDelivery(ackCtx, record.Token(), receipt)
			ackCancel()
			if ackErr == nil {
				result = telemetry.OutboxAcknowledged
				if !record.AcceptedAt.IsZero() {
					p.tel.Outbox().Lag(string(d), time.Since(record.AcceptedAt))
				}
				return
			}
			category = "unavailable"
		}
	}
	result = category
	if ctx.Err() != nil {
		return
	}
	p.state(d, 0, category)
	retryCtx, cancel := context.WithTimeout(ctx, p.config.StoreTimeout)
	defer cancel()
	if category == "conflict" {
		if err := p.store.BlockDelivery(retryCtx, record.Token()); err != nil {
			p.state(d, 0, "unavailable")
		}
		return
	}
	// Failed retry persistence leaves the original lease recoverable after expiry.
	if err := p.store.RetryDelivery(retryCtx, durable.DeliveryRetry{Token: record.Token(), Delay: p.backoff(record.Attempts), Category: category}); err != nil {
		p.state(d, 0, "unavailable")
	}
}
func accept(ctx context.Context, sink Sink, envelope durable.Delivery) (receipt durable.SinkReceipt, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("dispatch: sink panic")
		}
	}()
	return sink.Accept(ctx, envelope)
}
func (p *Publisher) backoff(attempts int64) time.Duration {
	delay := p.config.RetryMin
	for i := int64(1); i < attempts && delay < p.config.RetryMax; i++ {
		if delay > p.config.RetryMax/2 {
			return p.config.RetryMax
		}
		delay *= 2
	}
	if delay > p.config.RetryMax {
		return p.config.RetryMax
	}
	return delay
}
