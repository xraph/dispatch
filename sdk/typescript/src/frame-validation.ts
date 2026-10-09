import type { Frame } from "./frame";

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isNonemptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

// A malformed correl_id can still be identified by a matching frame id.
export function correlationID(
  frame: Record<string, unknown>,
): string | undefined {
  if (isNonemptyString(frame.correl_id)) return frame.correl_id;
  if (isNonemptyString(frame.id)) return frame.id;
  return undefined;
}

export function isFrame(
  value: Record<string, unknown>,
): value is Record<string, unknown> & Frame {
  if (!isNonemptyString(value.id) || typeof value.ts !== "string") return false;
  switch (value.type) {
    case "request":
    case "response":
    case "event":
    case "error":
    case "ping":
    case "pong":
      break;
    default:
      return false;
  }
  if (value.correl_id !== undefined && !isNonemptyString(value.correl_id))
    return false;
  for (const field of ["method", "token", "app_id", "org_id", "channel"]) {
    if (value[field] !== undefined && typeof value[field] !== "string")
      return false;
  }
  if (value.credits !== undefined && !Number.isSafeInteger(value.credits))
    return false;
  if (value.type === "error" || value.error !== undefined) {
    if (
      !isRecord(value.error) ||
      !Number.isSafeInteger(value.error.code) ||
      typeof value.error.message !== "string"
    )
      return false;
  }
  return true;
}
