// Package metricstest is a go-utils metric factory for Dispatch's tests. It
// keys series by name and labels, as Forge's registry does. go-utils' own
// collector keys them by name alone, which folds every labelled series into
// the first one created.
package metricstest

import (
	"maps"
	"slices"
	"strings"
	"sync"

	gu "github.com/xraph/go-utils/metrics"
)

// Factory records every series created through it.
type Factory struct {
	mu         sync.Mutex
	labels     map[string][]map[string]string
	counters   map[string]gu.Counter
	gauges     map[string]gu.Gauge
	histograms map[string]gu.Histogram
}

var _ gu.MetricFactory = (*Factory)(nil)

// New returns an empty Factory.
func New() *Factory {
	return &Factory{
		labels:     map[string][]map[string]string{},
		counters:   map[string]gu.Counter{},
		gauges:     map[string]gu.Gauge{},
		histograms: map[string]gu.Histogram{},
	}
}

// Key names one series: its name, then its labels sorted by key.
func Key(name string, labels map[string]string) string {
	var b strings.Builder
	b.WriteString(name)
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		b.WriteString("|" + k + "=" + labels[k])
	}
	return b.String()
}

func (f *Factory) register(name string, opts []gu.MetricOption) string {
	o := &gu.MetricOptions{}
	for _, opt := range opts {
		opt(o)
	}
	labels := maps.Clone(o.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	key := Key(name, labels)
	if !slices.ContainsFunc(f.labels[name], func(l map[string]string) bool { return Key(name, l) == key }) {
		f.labels[name] = append(f.labels[name], labels)
	}
	return key
}

// Counter implements gu.MetricFactory.
func (f *Factory) Counter(name string, opts ...gu.MetricOption) gu.Counter {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := f.register(name, opts)
	if c, ok := f.counters[key]; ok {
		return c
	}
	c := gu.NewCounter(name, opts...)
	f.counters[key] = c
	return c
}

// Gauge implements gu.MetricFactory.
func (f *Factory) Gauge(name string, opts ...gu.MetricOption) gu.Gauge {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := f.register(name, opts)
	if g, ok := f.gauges[key]; ok {
		return g
	}
	g := gu.NewGauge(name, opts...)
	f.gauges[key] = g
	return g
}

// Histogram implements gu.MetricFactory.
func (f *Factory) Histogram(name string, opts ...gu.MetricOption) gu.Histogram {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := f.register(name, opts)
	if h, ok := f.histograms[key]; ok {
		return h
	}
	h := gu.NewHistogram(name, opts...)
	f.histograms[key] = h
	return h
}

// Summary implements gu.MetricFactory. Dispatch records no summaries.
func (f *Factory) Summary(name string, opts ...gu.MetricOption) gu.Summary {
	return gu.NewSummary(name, opts...)
}

// Timer implements gu.MetricFactory. Dispatch records no timers.
func (f *Factory) Timer(name string, opts ...gu.MetricOption) gu.Timer {
	return gu.NewTimer(name, opts...)
}

// Names returns every metric name created through f, sorted.
func (f *Factory) Names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.labels))
}

// Series returns the label sets created under name, in creation order.
func (f *Factory) Series(name string) []map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.labels[name])
}

// CounterValue returns the value of the counter series name{labels}, and
// whether it exists. Pass labels as key, value pairs.
func (f *Factory) CounterValue(name string, labels ...string) (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.counters[Key(name, pairs(labels))]
	if !ok {
		return 0, false
	}
	return c.Value(), true
}

// GaugeValue returns the value of the gauge series name{labels}, and whether
// it exists.
func (f *Factory) GaugeValue(name string, labels ...string) (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.gauges[Key(name, pairs(labels))]
	if !ok {
		return 0, false
	}
	return g.Value(), true
}

// HistogramCount returns the observation count of the histogram series
// name{labels}, and whether it exists.
func (f *Factory) HistogramCount(name string, labels ...string) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.histograms[Key(name, pairs(labels))]
	if !ok {
		return 0, false
	}
	return h.Count(), true
}

func pairs(kv []string) map[string]string {
	out := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}
