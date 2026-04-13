// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Int16", () => {
    it("round-trips signed 128-bit boundaries as bigint strings", () => {
        const frames: WireFrame[] = [
            {
                columns: [
                    {
                        name: "v",
                        type: "Int16",
                        payload: [
                            "-170141183460469231731687303715884105728",
                            "-1",
                            "0",
                            "1",
                            "170141183460469231731687303715884105727",
                        ],
                    },
                ],
            },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
