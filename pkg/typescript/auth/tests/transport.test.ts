// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from "vitest";

import {
  httpTransport,
  jsonHttpTransport,
  jsonWsTransport,
  wsTransport,
} from "../src/transport";

describe("prebuilt transports", () => {
  it("declare the right transport kind", () => {
    expect(wsTransport.kind).toBe("ws");
    expect(httpTransport.kind).toBe("http");
    expect(jsonWsTransport.kind).toBe("ws");
    expect(jsonHttpTransport.kind).toBe("http");
  });

  it("release is a no-op for http transports (no disconnect call)", () => {
    const calls: string[] = [];
    const fakeHttp = {
      disconnect: () => calls.push("disconnect-http"),
    };
    const fakeJsonHttp = {
      disconnect: () => calls.push("disconnect-json-http"),
    };
    expect(() => httpTransport.release(fakeHttp as never)).not.toThrow();
    expect(() => jsonHttpTransport.release(fakeJsonHttp as never)).not.toThrow();
    // HTTP transports must never touch disconnect; the field is incidental on
    // the real HttpClient but auth-package contract says release is a no-op.
    expect(calls).toEqual([]);
  });

  it("release calls disconnect on ws transports", () => {
    const calls: string[] = [];
    const fakeWs = {
      disconnect: () => calls.push("disconnect-ws"),
    };
    const fakeJsonWs = {
      disconnect: () => calls.push("disconnect-json-ws"),
    };
    wsTransport.release(fakeWs as never);
    jsonWsTransport.release(fakeJsonWs as never);
    expect(calls).toEqual(["disconnect-ws", "disconnect-json-ws"]);
  });

  it("release swallows disconnect errors", () => {
    const wsThrows = {
      disconnect: () => {
        throw new Error("kaboom");
      },
    };
    expect(() => wsTransport.release(wsThrows as never)).not.toThrow();
  });

  it("release is idempotent when disconnect is missing", () => {
    expect(() => wsTransport.release({} as never)).not.toThrow();
    expect(() => jsonWsTransport.release({} as never)).not.toThrow();
  });
});
