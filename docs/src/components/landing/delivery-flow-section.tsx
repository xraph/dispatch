"use client";

import { AnimatePresence, motion } from "framer-motion";
import { useEffect, useState } from "react";
import { cn } from "@/lib/cn";
import { SectionHeader } from "./section-header";

// ─── Job Type Cycler ─────────────────────────────────────────
const jobTypes = ["email.send", "report.generate", "user.onboard"];

function CyclingJobType() {
  const [index, setIndex] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      setIndex((prev) => (prev + 1) % jobTypes.length);
    }, 3500);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="relative h-5 overflow-hidden">
      <AnimatePresence mode="wait">
        <motion.span
          key={jobTypes[index]}
          initial={{ y: 12, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: -12, opacity: 0 }}
          transition={{ duration: 0.3 }}
          className="absolute inset-0 text-teal-500 dark:text-teal-400 font-mono text-xs font-medium"
        >
          {jobTypes[index]}
        </motion.span>
      </AnimatePresence>
    </div>
  );
}

// ─── Pipeline Stage ──────────────────────────────────────────
interface StageProps {
  label: string;
  sublabel?: React.ReactNode;
  color: string;
  borderColor: string;
  bgColor: string;
  pulse?: boolean;
  delay: number;
}

function Stage({
  label,
  sublabel,
  color,
  borderColor,
  bgColor,
  pulse,
  delay,
}: StageProps) {
  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.85 }}
      whileInView={{ opacity: 1, scale: 1 }}
      viewport={{ once: true }}
      transition={{ duration: 0.4, delay }}
      className={cn(
        "relative flex flex-col items-center gap-1 rounded-xl border px-4 py-3 min-w-[90px]",
        borderColor,
        bgColor,
      )}
    >
      {pulse && (
        <motion.div
          className={cn("absolute inset-0 rounded-xl border", borderColor)}
          animate={{ opacity: [0.4, 0], scale: [1, 1.12] }}
          transition={{ duration: 2, repeat: Infinity, ease: "easeOut" }}
        />
      )}
      <span className={cn("text-xs font-semibold font-mono", color)}>
        {label}
      </span>
      {sublabel && (
        <span className="text-[10px] text-fd-muted-foreground">{sublabel}</span>
      )}
    </motion.div>
  );
}

// ─── Animated Connection ─────────────────────────────────────
function Connection({
  color = "amber",
  delay = 0,
  horizontal = true,
}: {
  color?: "teal" | "amber" | "green" | "red";
  delay?: number;
  horizontal?: boolean;
}) {
  const colorMap = {
    teal: { line: "bg-teal-500/30", particle: "bg-teal-400" },
    amber: { line: "bg-amber-500/30", particle: "bg-amber-400" },
    green: { line: "bg-green-500/30", particle: "bg-green-400" },
    red: { line: "bg-red-500/30", particle: "bg-red-400" },
  };

  const c = colorMap[color];

  if (!horizontal) {
    return (
      <div className="relative flex flex-col items-center h-6 w-px">
        <div
          className={cn("absolute inset-0 w-[1.5px] rounded-full", c.line)}
        />
        <motion.div
          className={cn("absolute size-1.5 rounded-full", c.particle)}
          animate={{ y: [-2, 22], opacity: [0, 1, 1, 0] }}
          transition={{
            duration: 1.2,
            repeat: Infinity,
            ease: "linear",
            delay,
          }}
        />
      </div>
    );
  }

  return (
    <div className="relative flex items-center h-px w-8 md:w-12 shrink-0">
      <div
        className={cn(
          "absolute inset-0 h-[1.5px] rounded-full my-auto",
          c.line,
        )}
      />
      <motion.div
        className={cn("absolute size-1.5 rounded-full", c.particle)}
        animate={{ x: [-2, 40], opacity: [0, 1, 1, 0] }}
        transition={{ duration: 1.4, repeat: Infinity, ease: "linear", delay }}
      />
      {/* Arrow */}
      <div
        className="absolute right-0 border-l-[4px] border-y-[2.5px] border-y-transparent border-l-current opacity-30"
        style={{
          color:
            color === "teal"
              ? "#14b8a6"
              : color === "amber"
                ? "#f59e0b"
                : color === "green"
                  ? "#22c55e"
                  : "#ef4444",
        }}
      />
    </div>
  );
}

// ─── Endpoint Row ────────────────────────────────────────────
function EndpointRow({
  name,
  status,
  statusLabel,
  lineColor,
  delay,
}: {
  name: string;
  status: "delivered" | "retry" | "dlq";
  statusLabel: string;
  lineColor: "green" | "teal" | "amber" | "red";
  delay: number;
}) {
  const statusColors = {
    delivered:
      "text-green-600 dark:text-green-400 bg-green-500/10 border-green-500/20",
    retry:
      "text-amber-600 dark:text-amber-400 bg-amber-500/10 border-amber-500/20",
    dlq: "text-red-600 dark:text-red-400 bg-red-500/10 border-red-500/20",
  };

  return (
    <motion.div
      initial={{ opacity: 0, x: -8 }}
      whileInView={{ opacity: 1, x: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.4, delay }}
      className="flex items-center gap-0"
    >
      <Connection color={lineColor} delay={delay * 2} />
      <div className="rounded-lg border border-fd-border bg-fd-card/60 px-3 py-1.5 font-mono text-[10px] text-fd-muted-foreground min-w-[100px] text-center">
        {name}
      </div>
      <Connection color={lineColor} delay={delay * 2 + 0.5} />
      <div
        className={cn(
          "rounded-md border px-2 py-1 font-mono text-[10px] font-medium whitespace-nowrap",
          statusColors[status],
        )}
      >
        {statusLabel}
      </div>
    </motion.div>
  );
}

// ─── Animated Pipeline Diagram ───────────────────────────────
function PipelineDiagram() {
  const [retryResolved, setRetryResolved] = useState(false);

  useEffect(() => {
    const interval = setInterval(() => {
      setRetryResolved((prev) => !prev);
    }, 4000);
    return () => clearInterval(interval);
  }, []);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      whileInView={{ opacity: 1 }}
      viewport={{ once: true }}
      transition={{ duration: 0.6 }}
      className="relative"
    >
      {/* Background glow */}
      <div className="absolute inset-0 -m-6 bg-gradient-to-br from-teal-500/5 via-transparent to-cyan-500/5 rounded-3xl blur-xl" />

      <div className="relative p-3 sm:p-6 rounded-2xl border border-fd-border/50 bg-fd-card/30 backdrop-blur-sm">
        {/* Pipeline stages - horizontal on desktop */}
        <div className="flex flex-col items-center gap-4">
          {/* Stage 1: Top pipeline stages */}
          <div className="flex items-center gap-0 flex-wrap justify-center">
            <Stage
              label="Enqueue()"
              sublabel={<CyclingJobType />}
              color="text-teal-600 dark:text-teal-400"
              borderColor="border-teal-500/30"
              bgColor="bg-teal-500/5"
              delay={0.1}
            />
            <Connection color="teal" delay={0} />
            <Stage
              label="Schedule"
              sublabel="queue"
              color="text-purple-600 dark:text-purple-400"
              borderColor="border-purple-500/30"
              bgColor="bg-purple-500/5"
              delay={0.2}
            />
            <Connection color="teal" delay={0.5} />
            <Stage
              label="Execute"
              sublabel="process"
              color="text-teal-600 dark:text-teal-400"
              borderColor="border-teal-500/30"
              bgColor="bg-teal-500/8"
              pulse
              delay={0.3}
            />
          </div>

          {/* Vertical connection from fan-out to endpoints */}
          <Connection color="teal" horizontal={false} delay={1} />

          {/* Stage 2: Endpoints with results */}
          <div className="flex flex-col items-start gap-2.5">
            <EndpointRow
              name="worker-01"
              status="delivered"
              statusLabel="✓ Completed"
              lineColor="green"
              delay={0.5}
            />
            <EndpointRow
              name="worker-02"
              status={retryResolved ? "delivered" : "retry"}
              statusLabel={retryResolved ? "✓ Completed" : "↻ Retry"}
              lineColor={retryResolved ? "green" : "amber"}
              delay={0.6}
            />
            <EndpointRow
              name="worker-03"
              status="dlq"
              statusLabel="✗ → DLQ"
              lineColor="red"
              delay={0.7}
            />
          </div>

          {/* Legend */}
          <div className="flex items-center gap-4 mt-4 text-[10px] text-fd-muted-foreground">
            <div className="flex items-center gap-1.5">
              <div className="size-2 rounded-full bg-green-500" />
              <span>Completed</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="size-2 rounded-full bg-amber-500" />
              <span>Retry</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="size-2 rounded-full bg-red-500" />
              <span>DLQ</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="size-2 rounded-full bg-gray-400" />
              <span>Disabled</span>
            </div>
          </div>
        </div>
      </div>
    </motion.div>
  );
}

// ─── Feature Bullet ──────────────────────────────────────────
function FeatureBullet({
  title,
  description,
  delay,
}: {
  title: string;
  description: string;
  delay: number;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, x: -10 }}
      whileInView={{ opacity: 1, x: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.4, delay }}
      className="flex items-start gap-3"
    >
      <div className="mt-1 flex items-center justify-center size-5 rounded-md bg-teal-500/10 shrink-0">
        <svg
          className="size-3 text-teal-500"
          viewBox="0 0 12 12"
          fill="none"
          aria-hidden="true"
        >
          <path
            d="M2 6l3 3 5-5"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </div>
      <div>
        <h4 className="text-sm font-semibold text-fd-foreground">{title}</h4>
        <p className="text-xs text-fd-muted-foreground mt-0.5 leading-relaxed">
          {description}
        </p>
      </div>
    </motion.div>
  );
}

// ─── Delivery Flow Section ───────────────────────────────────
export function DeliveryFlowSection() {
  return (
    <section className="relative w-full py-20 sm:py-28 overflow-hidden">
      {/* Background */}
      <div className="absolute inset-0 bg-gradient-to-b from-transparent via-teal-500/[0.02] to-transparent" />

      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <div className="grid gap-12 lg:grid-cols-2 lg:gap-16 items-center">
          {/* Left: Text content */}
          <div className="flex flex-col">
            <SectionHeader
              badge="Execution Pipeline"
              title="From enqueue to execution. Automatically."
              description="Dispatch orchestrates the entire job lifecycle — scheduling, execution, retries, and dead-letter routing."
              align="left"
            />

            <div className="mt-8 space-y-5">
              <FeatureBullet
                title="Type-Safe Jobs"
                description="Every job is typed and validated before execution. Mismatched payloads fail at compile time, not runtime."
                delay={0.2}
              />
              <FeatureBullet
                title="Priority Scheduling"
                description="Jobs are scheduled by priority and distributed to available workers. Each worker has independent capacity and backoff policies."
                delay={0.3}
              />
              <FeatureBullet
                title="Outcome Routing"
                description="Success = completed. Transient error = retry with backoff. Exhausted retries = dead letter queue for inspection and replay."
                delay={0.4}
              />
            </div>

            <motion.div
              initial={{ opacity: 0 }}
              whileInView={{ opacity: 1 }}
              viewport={{ once: true }}
              transition={{ delay: 0.5 }}
              className="mt-8"
            >
              <a
                href="/docs/architecture"
                className="inline-flex items-center gap-1 text-sm font-medium text-teal-600 dark:text-teal-400 hover:text-teal-500 transition-colors"
              >
                Learn about the architecture
                <svg
                  className="size-3.5"
                  viewBox="0 0 16 16"
                  fill="none"
                  aria-hidden="true"
                >
                  <path
                    d="M6 4l4 4-4 4"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </a>
            </motion.div>
          </div>

          {/* Right: Pipeline diagram */}
          <div className="relative">
            <PipelineDiagram />
          </div>
        </div>
      </div>
    </section>
  );
}
