/**
 * DWP frame types — mirrors dwp/frame.go.
 */

export type FrameType =
  | "request"
  | "response"
  | "event"
  | "error"
  | "ping"
  | "pong";

export interface Frame {
  id: string;
  type: FrameType;
  method?: string;
  correl_id?: string;
  token?: string;
  app_id?: string;
  org_id?: string;
  data?: unknown;
  error?: FrameError;
  channel?: string;
  credits?: number;
  ts: string;
}

export interface FrameError {
  code: number;
  message: string;
}

// DWP methods.
export const Method = {
  Auth: "auth",
  JobEnqueue: "job.enqueue",
  JobGet: "job.get",
  JobCancel: "job.cancel",
  JobList: "job.list",
  WorkflowStart: "workflow.start",
  WorkflowGet: "workflow.get",
  WorkflowEvent: "workflow.event",
  WorkflowCancel: "workflow.cancel",
  WorkflowTimeline: "workflow.timeline",
  WorkflowReplay: "workflow.replay",
  Subscribe: "subscribe",
  Unsubscribe: "unsubscribe",
  CronList: "cron.list",
  DLQList: "dlq.list",
  DLQReplay: "dlq.replay",
  Stats: "stats",
} as const;

export type MethodType = (typeof Method)[keyof typeof Method];

// Stream event types.
export interface StreamEvent {
  type: string;
  timestamp: string;
  topic: string;
  data: unknown;
}

let frameCounter = 0;

export function generateFrameID(): string {
  return `${Date.now()}-${++frameCounter}`;
}
