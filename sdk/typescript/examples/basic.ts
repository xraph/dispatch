/**
 * Basic Dispatch TypeScript SDK example.
 *
 * Demonstrates connecting to a Dispatch server, enqueueing jobs,
 * starting workflows, subscribing to events, and publishing events.
 *
 * Prerequisites:
 *   1. Start the kitchen-sink example server:
 *      cd _examples/kitchen-sink && go run .
 *
 *   2. Run this example:
 *      npx tsx examples/basic.ts
 *
 * The Dispatch server should be running on ws://localhost:8080/dwp.
 */

import { Dispatch } from "../src/index";

const SERVER_URL = "ws://localhost:8080/dwp";
const TOKEN = "demo-token";

async function main() {
  // ── 1. Connect to the Dispatch server ──────────────
  const client = new Dispatch({
    url: SERVER_URL,
    token: TOKEN,
  });

  const sessionId = await client.connect();
  console.log(`Connected! Session ID: ${sessionId}`);

  // ── 2. Enqueue a job ──────────────────────────────
  console.log("\n--- Enqueueing a job ---");
  const job = await client.enqueue("send-email", {
    to: "alice@example.com",
    subject: "Hello from TypeScript!",
  });
  console.log(`Job enqueued: ${JSON.stringify(job)}`);

  // Get job details.
  const jobDetails = await client.getJob(job.job_id);
  console.log(`Job details: ${JSON.stringify(jobDetails)}`);

  // ── 3. Enqueue with options ───────────────────────
  console.log("\n--- Enqueueing with options ---");
  const priorityJob = await client.enqueue(
    "process-image",
    { url: "https://example.com/photo.jpg", width: 800, height: 600 },
    { queue: "images", priority: 10 },
  );
  console.log(`Priority job: ${JSON.stringify(priorityJob)}`);

  // ── 4. Start a workflow ───────────────────────────
  console.log("\n--- Starting a workflow ---");
  const run = await client.startWorkflow("order-pipeline", {
    order_id: "TS-001",
    items: ["laptop", "mouse", "keyboard"],
  });
  console.log(`Workflow started: ${JSON.stringify(run)}`);

  // Get workflow details.
  const runDetails = await client.getWorkflow(run.run_id);
  console.log(`Workflow details: ${JSON.stringify(runDetails)}`);

  // ── 5. Get server stats ───────────────────────────
  console.log("\n--- Server stats ---");
  const stats = await client.stats();
  console.log(`Stats: ${JSON.stringify(stats, null, 2)}`);

  // ── 6. Start a saga workflow ──────────────────────
  console.log("\n--- Starting saga workflow ---");
  const saga = await client.startWorkflow("booking-saga", {
    trip_id: "TRIP-TS-001",
  });
  console.log(`Saga workflow: ${JSON.stringify(saga)}`);

  // ── 7. Start a batch workflow with child workflows ─
  console.log("\n--- Starting batch workflow ---");
  const batch = await client.startWorkflow("batch-process", {
    item_ids: ["item-a", "item-b", "item-c"],
  });
  console.log(`Batch workflow: ${JSON.stringify(batch)}`);

  // ── 8. Subscribe to events (with timeout) ─────────
  console.log("\n--- Subscribing to job events ---");

  // Subscribe in the background and collect a few events.
  const eventPromise = collectEvents(client, "jobs", 3, 5000);

  // Enqueue some jobs to generate events.
  await client.enqueue("send-email", {
    to: "bob@example.com",
    subject: "Event test 1",
  });
  await client.enqueue("send-email", {
    to: "carol@example.com",
    subject: "Event test 2",
  });
  await client.enqueue("send-email", {
    to: "dave@example.com",
    subject: "Event test 3",
  });

  const events = await eventPromise;
  console.log(`Received ${events.length} event(s):`);
  for (const event of events) {
    console.log(`  ${JSON.stringify(event)}`);
  }

  // ── Done ──────────────────────────────────────────
  console.log("\nAll operations completed successfully!");
  client.close();
}

/**
 * Subscribe to a channel and collect up to maxEvents events,
 * with a timeout to prevent hanging.
 */
async function collectEvents(
  client: Dispatch,
  channel: string,
  maxEvents: number,
  timeoutMs: number,
): Promise<unknown[]> {
  const events: unknown[] = [];

  return new Promise(async (resolve) => {
    const timer = setTimeout(() => {
      resolve(events);
    }, timeoutMs);

    try {
      for await (const event of client.subscribe(channel)) {
        events.push(event);
        if (events.length >= maxEvents) {
          clearTimeout(timer);
          resolve(events);
          break;
        }
      }
    } catch {
      clearTimeout(timer);
      resolve(events);
    }
  });
}

// Run the example.
main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
