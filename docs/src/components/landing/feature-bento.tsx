"use client";

import { motion } from "framer-motion";
import { cn } from "@/lib/cn";
import { CodeBlock } from "./code-block";
import { SectionHeader } from "./section-header";

interface FeatureCard {
  title: string;
  description: string;
  icon: React.ReactNode;
  code: string;
  filename: string;
  colSpan?: number;
}

const features: FeatureCard[] = [
  {
    title: "Background Jobs",
    description:
      "Define typed handlers, enqueue with priority, and retry with configurable backoff. No event is lost, ever.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M4 19.5A2.5 2.5 0 016.5 17H20" />
        <path d="M6.5 2H20v20H6.5A2.5 2.5 0 014 19.5v-15A2.5 2.5 0 016.5 2z" />
        <path d="M8 7h8M8 11h6" />
      </svg>
    ),
    code: `dispatch.Register("email.send",
  dispatch.WithHandler(sendEmail),
  dispatch.WithPriority(High),
  dispatch.WithRetry(5),
)`,
    filename: "jobs.go",
  },
  {
    title: "Durable Workflows",
    description:
      "Multi-step Go functions that checkpoint progress and resume after failure. No YAML, no DSL.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
        <path d="M9 12l2 2 4-4" />
      </svg>
    ),
    code: `dispatch.Workflow("onboard.user",
  dispatch.Step("validate", validateUser),
  dispatch.Step("provision", provisionAccount),
  dispatch.Step("notify", notifyUser),
)`,
    filename: "workflows.go",
  },
  {
    title: "Dead Letter Queue",
    description:
      "Jobs that exhaust retries are captured automatically. Inspect, debug, and replay them on demand.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <rect x="2" y="4" width="20" height="5" rx="1" />
        <path d="M4 9v9a2 2 0 002 2h12a2 2 0 002-2V9" />
        <path d="M10 13h4" />
      </svg>
    ),
    code: `items, _ := d.DLQ().List(ctx,
  dlq.WithJobType("email.send"),
  dlq.WithLimit(10),
)
d.DLQ().Replay(ctx, items[0].ID)`,
    filename: "dlq.go",
  },
  {
    title: "Distributed Cron",
    description:
      "Leader-elected cron scheduling with per-tenant support and runtime enable/disable.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <circle cx="12" cy="12" r="10" />
        <path d="M12 6v6l4 2" />
      </svg>
    ),
    code: `dispatch.Cron("daily.report",
  dispatch.WithSchedule("0 9 * * *"),
  dispatch.WithLeaderElection(true),
  dispatch.WithTenant(tenantID),
)`,
    filename: "cron.go",
  },
  {
    title: "Distributed Workers",
    description:
      "Worker registration, heartbeats, leader election, and automatic work stealing across nodes.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83" />
        <circle cx="12" cy="12" r="3" />
      </svg>
    ),
    code: `d.Workers().Register(ctx, dispatch.Worker{
  ID:        workerID,
  Capacity:  10,
  Heartbeat: 30 * time.Second,
})`,
    filename: "workers.go",
  },
  {
    title: "Pluggable Stores",
    description:
      "Ship with in-memory for dev, swap to PostgreSQL for production. Bring your own store with a simple interface.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <ellipse cx="12" cy="5" rx="9" ry="3" />
        <path d="M21 12c0 1.66-4.03 3-9 3s-9-1.34-9-3" />
        <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5" />
      </svg>
    ),
    code: `d, _ := dispatch.New(
  dispatch.WithStore(postgres.New(db)),
  dispatch.WithWorkers(8),
  dispatch.WithLogger(slog.Default()),
)`,
    filename: "main.go",
    colSpan: 2,
  },
];

const containerVariants = {
  hidden: {},
  visible: {
    transition: {
      staggerChildren: 0.08,
    },
  },
};

const itemVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.5, ease: "easeOut" as const },
  },
};

export function FeatureBento() {
  return (
    <section className="relative w-full py-20 sm:py-28">
      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <SectionHeader
          badge="Features"
          title="Everything you need for background processing"
          description="Dispatch handles the hard parts — retries, scheduling, dead letters, worker coordination — so you can focus on your business logic."
        />

        <motion.div
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: "-50px" }}
          className="mt-14 grid grid-cols-1 md:grid-cols-2 gap-4"
        >
          {features.map((feature) => (
            <motion.div
              key={feature.title}
              variants={itemVariants}
              className={cn(
                "group relative rounded-xl border border-fd-border bg-fd-card/50 backdrop-blur-sm p-6 hover:border-teal-500/20 hover:bg-fd-card/80 transition-all duration-300",
                feature.colSpan === 2 && "md:col-span-2",
              )}
            >
              {/* Header */}
              <div className="flex items-start gap-3 mb-4">
                <div className="flex items-center justify-center size-9 rounded-lg bg-teal-500/10 text-teal-600 dark:text-teal-400 shrink-0">
                  {feature.icon}
                </div>
                <div>
                  <h3 className="text-sm font-semibold text-fd-foreground">
                    {feature.title}
                  </h3>
                  <p className="text-xs text-fd-muted-foreground mt-1 leading-relaxed">
                    {feature.description}
                  </p>
                </div>
              </div>

              {/* Code snippet */}
              <CodeBlock
                code={feature.code}
                filename={feature.filename}
                showLineNumbers={false}
                className="text-xs"
              />
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}
