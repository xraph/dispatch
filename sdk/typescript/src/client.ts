/**
 * Dispatch client — main entry point for the TypeScript SDK.
 */

import { Transport, type TransportOptions } from "./transport";
import { Method, type StreamEvent } from "./frame";

export interface DispatchOptions {
  url: string;
  token: string;
  format?: "json" | "msgpack";
  reconnect?: boolean;
  maxRetries?: number;
}

export interface JobResult {
  job_id: string;
  queue: string;
  state: string;
}

export interface WorkflowResult {
  run_id: string;
  name: string;
  state: string;
}

export interface EnqueueOptions {
  queue?: string;
  priority?: number;
}

/**
 * Dispatch is the main client class for communicating with a Dispatch server
 * via the Dispatch Wire Protocol (DWP).
 *
 * @example
 * ```ts
 * const dispatch = new Dispatch({
 *   url: 'wss://api.example.com/dwp',
 *   token: 'dk_...',
 * });
 * await dispatch.connect();
 *
 * const job = await dispatch.enqueue('send-email', { to: 'user@example.com' });
 * const run = await dispatch.startWorkflow('order-pipeline', { orderId: '123' });
 *
 * for await (const event of dispatch.watch(run.run_id)) {
 *   console.log(`${event.type}: ${JSON.stringify(event.data)}`);
 * }
 * ```
 */
export class Dispatch {
  private transport: Transport;

  constructor(options: DispatchOptions) {
    this.transport = new Transport(options as TransportOptions);
  }

  /**
   * Connect to the DWP server and authenticate.
   * @returns The session ID assigned by the server.
   */
  async connect(): Promise<string> {
    return this.transport.connect();
  }

  /** Get the current session ID. */
  get sessionId(): string {
    return this.transport.getSessionId();
  }

  // ── Jobs ─────────────────────────────────

  /**
   * Enqueue a job on the remote dispatch server.
   */
  async enqueue(
    name: string,
    payload: unknown,
    options?: EnqueueOptions,
  ): Promise<JobResult> {
    const resp = await this.transport.request(Method.JobEnqueue, {
      name,
      payload,
      queue: options?.queue,
      priority: options?.priority,
    });
    return resp.data as JobResult;
  }

  /**
   * Get a job by ID.
   */
  async getJob(jobId: string): Promise<unknown> {
    const resp = await this.transport.request(Method.JobGet, {
      job_id: jobId,
    });
    return resp.data;
  }

  /**
   * Cancel a job by ID.
   */
  async cancelJob(jobId: string): Promise<void> {
    await this.transport.request(Method.JobCancel, { job_id: jobId });
  }

  // ── Workflows ────────────────────────────

  /**
   * Start a workflow run.
   */
  async startWorkflow(
    name: string,
    input: unknown,
  ): Promise<WorkflowResult> {
    const resp = await this.transport.request(Method.WorkflowStart, {
      name,
      input: typeof input === "string" ? input : JSON.stringify(input),
    });
    return resp.data as WorkflowResult;
  }

  /**
   * Get a workflow run by ID.
   */
  async getWorkflow(runId: string): Promise<unknown> {
    const resp = await this.transport.request(Method.WorkflowGet, {
      run_id: runId,
    });
    return resp.data;
  }

  /**
   * Publish an event that can trigger workflow continuations.
   */
  async publishEvent(name: string, payload: unknown): Promise<void> {
    await this.transport.request(Method.WorkflowEvent, {
      name,
      payload: typeof payload === "string" ? payload : JSON.stringify(payload),
    });
  }

  /**
   * Get the step timeline for a workflow run.
   */
  async workflowTimeline(runId: string): Promise<unknown> {
    const resp = await this.transport.request(Method.WorkflowTimeline, {
      run_id: runId,
    });
    return resp.data;
  }

  // ── Subscriptions ────────────────────────

  /**
   * Subscribe to a stream channel. Returns an AsyncIterator that yields
   * events as they arrive.
   *
   * @example
   * ```ts
   * for await (const event of dispatch.subscribe('queue:critical')) {
   *   console.log(event);
   * }
   * ```
   */
  async *subscribe(channel: string): AsyncGenerator<StreamEvent> {
    await this.transport.request(Method.Subscribe, { channel });

    const queue: StreamEvent[] = [];
    let resolve: (() => void) | null = null;
    let done = false;

    this.transport.onEvent(channel, (event) => {
      queue.push(event);
      resolve?.();
    });

    try {
      while (!done) {
        if (queue.length > 0) {
          yield queue.shift()!;
        } else {
          await new Promise<void>((r) => {
            resolve = r;
          });
          resolve = null;
        }
      }
    } finally {
      this.transport.removeEvent(channel);
      // Best-effort unsubscribe.
      this.transport
        .request(Method.Unsubscribe, { channel })
        .catch(() => {});
    }
  }

  /**
   * Watch a specific workflow run for real-time events.
   * Convenience for `subscribe('workflow:<runId>')`.
   */
  async *watch(runId: string): AsyncGenerator<StreamEvent> {
    yield* this.subscribe(`workflow:${runId}`);
  }

  // ── Admin ────────────────────────────────

  /**
   * Get broker and connection statistics.
   */
  async stats(): Promise<unknown> {
    const resp = await this.transport.request(Method.Stats);
    return resp.data;
  }

  /**
   * Close the connection.
   */
  close() {
    this.transport.close();
  }
}
