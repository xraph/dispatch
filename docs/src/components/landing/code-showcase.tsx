"use client";

import { motion } from "framer-motion";
import { CodeBlock } from "./code-block";
import { SectionHeader } from "./section-header";

const sendCode = `package main

import (
  "log/slog"
  "github.com/xraph/relay"
  "github.com/xraph/relay/store/memory"
)

func main() {
  r, _ := relay.New(
    relay.WithStore(memory.New()),
    relay.WithWorkers(4),
    relay.WithLogger(slog.Default()),
  )

  // Register an event type
  r.Catalog().Register("order.created")

  // Register an endpoint
  r.Endpoints().Create(ctx, relay.Endpoint{
    URL: "https://api.acme.co/webhooks",
    EventTypes: []string{"order.created"},
  })

  // Send an event
  r.Send(ctx, relay.Event{
    Type: "order.created",
    Payload: orderJSON,
  })
}`;

const verifyCode = `package main

import (
  "crypto/hmac"
  "crypto/sha256"
  "encoding/hex"
  "io"
  "net/http"
)

func webhookHandler(w http.ResponseWriter, r *http.Request) {
  body, _ := io.ReadAll(r.Body)
  signature := r.Header.Get("X-Relay-Signature")

  // Verify HMAC-SHA256 signature
  mac := hmac.New(sha256.New, []byte(secret))
  mac.Write(body)
  expected := hex.EncodeToString(mac.Sum(nil))

  if !hmac.Equal([]byte(signature), []byte(expected)) {
    http.Error(w, "invalid signature", 401)
    return
  }

  // Process the verified webhook
  processEvent(body)
  w.WriteHeader(200)
}`;

export function CodeShowcase() {
  return (
    <section className="relative w-full py-20 sm:py-28">
      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <SectionHeader
          badge="Developer Experience"
          title="Simple API. Production power."
          description="Send your first webhook in under 20 lines. Verify signatures on the receiver side with standard crypto."
        />

        <div className="mt-14 grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Sender side */}
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.1 }}
          >
            <div className="mb-3 flex items-center gap-2">
              <div className="size-2 rounded-full bg-teal-500" />
              <span className="text-xs font-medium text-fd-muted-foreground uppercase tracking-wider">
                Sender
              </span>
            </div>
            <CodeBlock code={sendCode} filename="main.go" />
          </motion.div>

          {/* Receiver side */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
          >
            <div className="mb-3 flex items-center gap-2">
              <div className="size-2 rounded-full bg-green-500" />
              <span className="text-xs font-medium text-fd-muted-foreground uppercase tracking-wider">
                Receiver
              </span>
            </div>
            <CodeBlock code={verifyCode} filename="receiver.go" />
          </motion.div>
        </div>
      </div>
    </section>
  );
}
