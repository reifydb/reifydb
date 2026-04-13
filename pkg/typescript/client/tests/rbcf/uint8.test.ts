// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Uint8", () => {
    it("round-trips unsigned 64-bit boundaries as bigint strings", () => {
        const frames: WireFrame[] = [
            {
                columns: [
                    {
                        name: "v",
                        type: "Uint8",
                        payload: ["0", "1", "9223372036854775807", "18446744073709551615"],
                    },
                ],
            },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
