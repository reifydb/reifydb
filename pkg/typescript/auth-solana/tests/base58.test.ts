// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";

import { encode_base58, is_base58 } from "../src/base58";

function bytes_from(hex: string): Uint8Array {
  const clean = hex.replace(/\s+/g, "");
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

describe("encode_base58", () => {
  // Canonical Bitcoin / Solana base58 vectors. Source: well-known reference
  // implementations (e.g. bitcoinjs-lib bs58 README).
  it.each<[string, string]>([
    ["", ""],
    ["00", "1"],
    ["0000", "11"],
    ["00000000", "1111"],
    ["61", "2g"],
    ["626262", "a3gV"],
    ["636363", "aPEr"],
    ["73696d706c792061206c6f6e6720737472696e67", "2cFupjhnEsSn59qHXstmK2ffpLv2"],
    [
      "00eb15231dfceb60925886b67d065299925915aeb172c06647",
      "1NS17iag9jJgTHD1VXjvLCEnZuQ3rJDE9L",
    ],
  ])("encodes hex %s -> %s", (hex, expected) => {
    expect(encode_base58(bytes_from(hex))).toBe(expected);
  });

  it("encodes long leading-zero runs as leading '1's", () => {
    expect(encode_base58(new Uint8Array(8))).toBe("11111111");
  });

  it("does not emit ambiguous characters (0, O, I, l)", () => {
    const sample = new Uint8Array(64);
    for (let i = 0; i < sample.length; i += 1) sample[i] = i;
    const out = encode_base58(sample);
    expect(out).not.toContain("0");
    expect(out).not.toContain("O");
    expect(out).not.toContain("I");
    expect(out).not.toContain("l");
  });
});

describe("is_base58", () => {
  it("accepts valid alphabet strings", () => {
    expect(is_base58("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")).toBe(true);
    expect(is_base58("a3gV")).toBe(true);
    expect(is_base58("1NS17iag9jJgTHD1VXjvLCEnZuQ3rJDE9L")).toBe(true);
  });

  it("rejects strings with ambiguous characters", () => {
    expect(is_base58("0OIl")).toBe(false);
    expect(is_base58("A0")).toBe(false);
  });

  it("rejects empty strings", () => {
    expect(is_base58("")).toBe(false);
  });
});
