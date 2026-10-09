package sinkhost

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/relay/signature"
)

func fmtSafe(message string) { fmt.Println(message) }
func serveReceiver(ctx context.Context, c Config) error {
	db, err := Open(ctx, c.DSNs["relay"])
	if err != nil {
		return err
	}
	defer db.Close()
	pg := pgdriver.Unwrap(db)
	if _, err = pg.Exec(ctx, `CREATE TABLE IF NOT EXISTS qualification_received(endpoint TEXT NOT NULL,event_id TEXT NOT NULL,delivery_id TEXT NOT NULL,payload BYTEA NOT NULL,received_at TIMESTAMPTZ DEFAULT clock_timestamp(),PRIMARY KEY(endpoint,event_id))`); err != nil {
		return err
	}
	router := forge.NewRouter()
	for _, path := range []string{"/one", "/two"} {
		if err := router.POST(path, func(f forge.Context) error {
			body, e := io.ReadAll(io.LimitReader(f.Request().Body, 65537))
			if e != nil || len(body) > 65536 {
				return f.String(400, "invalid delivery")
			}
			timestamp, e := strconv.ParseInt(f.Request().Header.Get("X-Relay-Timestamp"), 10, 64)
			if e != nil || time.Since(time.Unix(timestamp, 0)) > time.Minute || time.Until(time.Unix(timestamp, 0)) > time.Minute || !signature.Verify(body, c.WebhookSecret, timestamp, f.Request().Header.Get("X-Relay-Signature")) {
				return f.String(401, "invalid signature")
			}
			eventID, deliveryID := f.Request().Header.Get("X-Relay-Event-ID"), f.Request().Header.Get("X-Relay-Delivery-ID")
			if eventID == "" || deliveryID == "" {
				return f.String(400, "missing identity")
			}
			if _, e = pg.Exec(f.Context(), `INSERT INTO qualification_received(endpoint,event_id,delivery_id,payload) VALUES($1,$2,$3,$4) ON CONFLICT(endpoint,event_id) DO NOTHING`, path, eventID, deliveryID, body); e != nil {
				return f.String(503, "receiver unavailable")
			}
			return f.String(200, "accepted")
		}); err != nil {
			return err
		}
	}
	return listen(ctx, router, c.Addresses["receiver"], "receiver")
}
