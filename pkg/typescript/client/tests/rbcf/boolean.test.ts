// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Boolean", () => {
    it("round-trips true / false", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "ok", type: "Boolean", payload: ["true", "false", "true", "true", "false"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips a single row", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "ok", type: "Boolean", payload: ["true"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips Option<Boolean> with nones", () => {
        const frames: WireFrame[] = [
            {
                columns: [
                    { name: "ok", type: { Option: "Boolean" }, payload: ["true", "⟪none⟫", "false"] },
                ],
            },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
