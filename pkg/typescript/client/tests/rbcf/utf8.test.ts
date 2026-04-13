// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Utf8", () => {
    it("round-trips ASCII strings including empties", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "s", type: "Utf8", payload: ["", "a", "hello", "world"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips multi-byte UTF-8", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "s", type: "Utf8", payload: ["héllo", "café ☕", "日本語", "🦀"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips Option<Utf8> with nones", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "s", type: { Option: "Utf8" }, payload: ["hi", "⟪none⟫", ""] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
