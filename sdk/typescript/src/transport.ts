/**
 * WebSocket transport with reconnection and SSE fallback.
 */

import type { Frame, StreamEvent } from "./frame";
import { generateFrameID, Method } from "./frame";
import { AuthError, ConnectionError } from "./errors";

export interface TransportOptions {
  url: string;
  token: string;
  format?: string;
  reconnect?: boolean;
  maxRetries?: number;
  baseDelay?: number;
  maxDelay?: number;
}

type FrameHandler = (frame: Frame) => void;
type EventHandler = (event: StreamEvent) => void;
type CloseHandler = () => void;

export class Transport {
  private ws: WebSocket | null = null;
  private pending = new Map<string, (frame: Frame) => void>();
  private eventHandlers = new Map<string, EventHandler[]>();
  private onClose: CloseHandler | null = null;
  private closed = false;
  private sessionId = "";
  private retryCount = 0;
  private readonly opts: Required<TransportOptions>;

  constructor(options: TransportOptions) {
    this.opts = {
      format: "json",
      reconnect: true,
      maxRetries: 10,
      baseDelay: 1000,
      maxDelay: 30000,
      ...options,
    };
  }

  async connect(): Promise<string> {
    return new Promise<string>((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.opts.url);
      } catch (err) {
        reject(new ConnectionError(`Failed to create WebSocket: ${err}`));
        return;
      }

      this.ws.onopen = () => {
        // Send auth frame.
        const authFrame: Frame = {
          id: generateFrameID(),
          type: "request",
          method: Method.Auth,
          token: this.opts.token,
          data: {
            token: this.opts.token,
            format: this.opts.format,
          },
          ts: new Date().toISOString(),
        };

        this.pending.set(authFrame.id, (resp) => {
          if (resp.type === "error") {
            reject(
              new AuthError(resp.error?.message ?? "Authentication failed"),
            );
            return;
          }
          const data = resp.data as { session_id?: string; format?: string };
          this.sessionId = data?.session_id ?? "";
          this.retryCount = 0;
          resolve(this.sessionId);
        });

        this.ws!.send(JSON.stringify(authFrame));
      };

      this.ws.onmessage = (event) => {
        this.handleMessage(event.data as string);
      };

      this.ws.onerror = () => {
        reject(new ConnectionError("WebSocket connection failed"));
      };

      this.ws.onclose = () => {
        if (!this.closed && this.opts.reconnect) {
          this.tryReconnect();
        }
        this.onClose?.();
      };
    });
  }

  private handleMessage(raw: string) {
    let frame: Frame;
    try {
      frame = JSON.parse(raw);
    } catch {
      return;
    }

    switch (frame.type) {
      case "response":
      case "error": {
        const correlId = frame.correl_id ?? frame.id;
        const resolver = this.pending.get(correlId);
        if (resolver) {
          this.pending.delete(correlId);
          resolver(frame);
        }
        break;
      }
      case "event": {
        const channel = frame.channel ?? "";
        const handlers = this.eventHandlers.get(channel);
        if (handlers) {
          const evt = JSON.parse(raw) as StreamEvent;
          for (const handler of handlers) {
            handler(evt);
          }
        }
        break;
      }
      case "pong":
        break;
    }
  }

  private async tryReconnect() {
    while (this.retryCount < this.opts.maxRetries && !this.closed) {
      this.retryCount++;
      const delay = Math.min(
        this.opts.baseDelay * Math.pow(2, this.retryCount - 1),
        this.opts.maxDelay,
      );

      await new Promise((r) => setTimeout(r, delay));

      try {
        await this.connect();
        return;
      } catch {
        // Retry.
      }
    }
  }

  async request(method: string, data?: unknown): Promise<Frame> {
    return new Promise<Frame>((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new ConnectionError("Not connected"));
        return;
      }

      const frame: Frame = {
        id: generateFrameID(),
        type: "request",
        method,
        data,
        ts: new Date().toISOString(),
      };

      this.pending.set(frame.id, (resp) => {
        if (resp.type === "error") {
          reject(
            new (class extends Error {
              code = resp.error?.code;
            })(resp.error?.message ?? "Unknown DWP error"),
          );
          return;
        }
        resolve(resp);
      });

      this.ws.send(JSON.stringify(frame));
    });
  }

  onEvent(channel: string, handler: EventHandler) {
    const handlers = this.eventHandlers.get(channel) ?? [];
    handlers.push(handler);
    this.eventHandlers.set(channel, handlers);
  }

  removeEvent(channel: string) {
    this.eventHandlers.delete(channel);
  }

  setOnClose(handler: CloseHandler) {
    this.onClose = handler;
  }

  getSessionId(): string {
    return this.sessionId;
  }

  close() {
    this.closed = true;
    this.ws?.close();
    this.pending.clear();
    this.eventHandlers.clear();
  }
}
