"use client";

import { motion } from "framer-motion";
import { CodeBlock } from "./code-block";
import { SectionHeader } from "./section-header";

const sendCode = `package main

import (
  "log/slog"
  "github.com/xraph/dispatch"
  "github.com/xraph/dispatch/store/memory"
)

func main() {
  d, _ := dispatch.New(
    dispatch.WithStore(memory.New()),
    dispatch.WithWorkers(4),
    dispatch.WithLogger(slog.Default()),
  )

  // Register a typed job handler
  dispatch.Register(d, "email.send", sendEmail)

  // Enqueue a job with options
  d.Enqueue(ctx, dispatch.Job{
    Type:     "email.send",
    Payload:  emailJSON,
    Priority: dispatch.High,
  })
}`;

const handlerCode = `package main

import (
  "context"
  "github.com/xraph/dispatch"
)

type EmailInput struct {
  To      string \`json:"to"\`
  Subject string \`json:"subject"\`
  Body    string \`json:"body"\`
}

func sendEmail(
  ctx context.Context,
  job *dispatch.Job[EmailInput],
) error {
  input := job.Payload

  if err := mailer.Send(
    input.To, input.Subject, input.Body,
  ); err != nil {
    // Mark transient errors as retryable
    return dispatch.Retryable(err)
  }
  return nil
}`;

export function CodeShowcase() {
  return (
    <section className="relative w-full py-20 sm:py-28">
      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <SectionHeader
          badge="Developer Experience"
          title="Simple API. Production power."
          description="Enqueue your first job in under 20 lines. Handle typed payloads on the worker side with zero boilerplate."
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

          {/* Handler side */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
          >
            <div className="mb-3 flex items-center gap-2">
              <div className="size-2 rounded-full bg-green-500" />
              <span className="text-xs font-medium text-fd-muted-foreground uppercase tracking-wider">
                Handler
              </span>
            </div>
            <CodeBlock code={handlerCode} filename="handler.go" />
          </motion.div>
        </div>
      </div>
    </section>
  );
}
