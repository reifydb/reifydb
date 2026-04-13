// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";

describe("rbcf framing", () => {
    it("encodes and decodes an empty frame set", () => {
        const bytes = rbcf.encode([]);
        expect(rbcf.decode(bytes)).toEqual([]);
    });

    it("rejects bytes with an invalid magic", () => {
        const bad = new Uint8Array(16);
        expect(() => rbcf.decode(bad)).toThrow(/invalid magic/);
    });

    it("rejects bytes with an unsupported version", () => {
        const bytes = rbcf.encode([]);
        // Patch version bytes (offset 4, u16 LE) to 99.
        bytes[4] = 99;
        bytes[5] = 0;
        expect(() => rbcf.decode(bytes)).toThrow(/unsupported version/);
    });
});
