// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf, type WireFrame } from "../src/rbcf";

function frame(op?: 1 | 2 | 3): WireFrame {
    const f: WireFrame = { columns: [] };
    if (op !== undefined) f.op = op;
    return f;
}

describe("rbcf frame op", () => {
    it("decodes an absent op as absent", () => {
        // Byte 0 is a legitimate query frame, so rejecting unknown ops must leave it untouched.
        expect(rbcf.decode(rbcf.encode([frame()]))[0].op).toBeUndefined();
    });

    it("round trips every defined op", () => {
        // Guards the accepted range against an off-by-one while tightening the unknown-op path.
        for (const op of [1, 2, 3] as const) {
            expect(rbcf.decode(rbcf.encode([frame(op)]))[0].op).toBe(op);
        }
    });

    it("rejects an unknown op byte instead of dropping the change kind", () => {
        // An absent op reads as a query frame, so an unknown op byte must not silently decode as one.
        const withoutOp = rbcf.encode([frame()]);
        const bytes = rbcf.encode([frame(1)]);
        const differing = [...bytes.keys()].filter((at) => bytes[at] !== withoutOp[at]);
        expect(differing).toHaveLength(1);
        bytes[differing[0]] = 9;

        expect(() => rbcf.decode(bytes)).toThrow("RBCF: unknown frame op 9");
    });
});
