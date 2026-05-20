// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from "vitest";

import {
  http_transport,
  json_http_transport,
  json_ws_transport,
  ws_transport,
} from "../src/transport";

describe("prebuilt transports", () => {
  it("declare the right transport kind", () => {
    expect(ws_transport.kind).toBe("ws");
    expect(http_transport.kind).toBe("http");
    expect(json_ws_transport.kind).toBe("ws");
    expect(json_http_transport.kind).toBe("http");
  });

  it("release is a no-op for http transports (no disconnect call)", () => {
    const calls: string[] = [];
    const fake_http = {
      disconnect: () => calls.push("disconnect-http"),
    };
    const fake_json_http = {
      disconnect: () => calls.push("disconnect-json-http"),
    };
    expect(() => http_transport.release(fake_http as never)).not.toThrow();
    expect(() => json_http_transport.release(fake_json_http as never)).not.toThrow();
    // HTTP transports must never touch disconnect; the field is incidental on
    // the real HttpClient but auth-package contract says release is a no-op.
    expect(calls).toEqual([]);
  });

  it("release calls disconnect on ws transports", () => {
    const calls: string[] = [];
    const fake_ws = {
      disconnect: () => calls.push("disconnect-ws"),
    };
    const fake_json_ws = {
      disconnect: () => calls.push("disconnect-json-ws"),
    };
    ws_transport.release(fake_ws as never);
    json_ws_transport.release(fake_json_ws as never);
    expect(calls).toEqual(["disconnect-ws", "disconnect-json-ws"]);
  });

  it("release swallows disconnect errors", () => {
    const ws_throws = {
      disconnect: () => {
        throw new Error("kaboom");
      },
    };
    expect(() => ws_transport.release(ws_throws as never)).not.toThrow();
  });

  it("release is idempotent when disconnect is missing", () => {
    expect(() => ws_transport.release({} as never)).not.toThrow();
    expect(() => json_ws_transport.release({} as never)).not.toThrow();
  });
});
