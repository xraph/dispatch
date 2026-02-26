/**
 * @xraph/dispatch — TypeScript SDK for the Dispatch Wire Protocol (DWP).
 *
 * @example
 * ```ts
 * import { Dispatch } from '@xraph/dispatch';
 *
 * const client = new Dispatch({
 *   url: 'wss://api.example.com/dwp',
 *   token: 'dk_...',
 * });
 * await client.connect();
 *
 * const job = await client.enqueue('send-email', { to: 'user@example.com' });
 * const run = await client.startWorkflow('order-pipeline', { orderId: '123' });
 *
 * for await (const event of client.watch(run.run_id)) {
 *   console.log(event);
 * }
 * ```
 */

export {
  Dispatch,
  type DispatchOptions,
  type JobResult,
  type WorkflowResult,
  type EnqueueOptions,
} from "./client";

export {
  type Frame,
  type FrameType,
  type ErrorDetail,
  type StreamEvent,
  Method,
} from "./frame";

export {
  Transport,
  type TransportOptions,
} from "./transport";

export {
  DispatchError,
  AuthError,
  ConnectionError,
  TimeoutError,
} from "./errors";
