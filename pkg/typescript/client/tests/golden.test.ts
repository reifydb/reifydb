// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Golden-vector tests against the Rust codec fixtures under crates/codec/golden/.
// These pin the TS port byte-for-byte to the Rust ground truth:
//   (a) the ValueKind byte table (tag/kinds.json),
//   (b) a full RBCF message (frames/plain_mixed.bin),
//   (c) the canonical self-describing value codec (value/*.bin).
// The expectation table mirrors value_cases() in crates/codec/tests/golden.rs.
//
// Skipped golden fixtures:
//   - key/*.bin: the ordered key codec is not implemented in TypeScript (keys are
//     a storage-engine concern, not a client concern), so those fixtures are not
//     exercised here.
// No value/*.bin case is skipped: none-values decode to the NONE_VALUE sentinel
// (the TS client's missing-value representation; Rust Display prints "none"),
// and Type/List/Record/Tuple values decode to their Rust Display string forms.

import { describe, expect, it } from "vitest";
import * as path from "path";
import * as fs from "fs";
import { NONE_VALUE } from "@reifydb/core";
import { rbcf } from "../src/rbcf";
import { TYPE_CODE } from "../src/rbcf/format";
import { decode_any_value } from "../src/rbcf/encoding/plain";

const goldenRoot = path.resolve(__dirname, "../../../../crates/codec/golden");

if (!fs.existsSync(goldenRoot)) {
    throw new Error(`Golden directory not found at ${goldenRoot}`);
}

describe("RBCF golden vectors", () => {
    it("kind table matches crates/codec/golden/tag/kinds.json byte-for-byte", () => {
        const kinds: Record<string, number> = JSON.parse(
            fs.readFileSync(path.join(goldenRoot, "tag/kinds.json"), "utf-8")
        );
        expect(Object.keys(TYPE_CODE).sort()).toEqual(Object.keys(kinds).sort());
        for (const [name, byte] of Object.entries(kinds)) {
            expect(TYPE_CODE[name as keyof typeof TYPE_CODE], `kind ${name}`).toBe(byte);
        }
    });

    it("decodes frames/plain_mixed.bin", () => {
        const bytes = new Uint8Array(fs.readFileSync(path.join(goldenRoot, "frames/plain_mixed.bin")));
        const frames = rbcf.decode(bytes);
        expect(frames).toHaveLength(1);
        expect(frames[0].row_numbers).toBeUndefined();
        expect(frames[0].created_at).toBeUndefined();
        expect(frames[0].updated_at).toBeUndefined();
        expect(frames[0].columns).toEqual([
            { name: "bools", type: "Boolean", payload: ["true", "false", "true"] },
            { name: "ints", type: "Int4", payload: ["1", "2", "3"] },
            { name: "texts", type: "Utf8", payload: ["a", "bb", "ccc"] },
            { name: "anys", type: "Any", payload: ["9", NONE_VALUE, "x"] },
        ]);
    });

    // Mirrors value_cases() in crates/codec/tests/golden.rs.
    const valueCases: Array<[string, string]> = [
        ["none_any.bin", NONE_VALUE],
        ["none_duration.bin", NONE_VALUE],
        ["none_option_duration.bin", NONE_VALUE],
        ["none_option3_duration.bin", NONE_VALUE],
        ["none_record.bin", NONE_VALUE],
        ["boolean_true.bin", "true"],
        ["float4.bin", "3.5"],
        ["float8.bin", "-2.25"],
        ["int1_min.bin", "-128"],
        ["int2.bin", "-2"],
        ["int4.bin", "42"],
        ["int8_min.bin", "-9223372036854775808"],
        ["int16_max.bin", "170141183460469231731687303715884105727"],
        ["utf8.bin", "reify"],
        ["uint1.bin", "1"],
        ["uint2.bin", "2"],
        ["uint4.bin", "4"],
        ["uint8.bin", "8"],
        ["uint16_max.bin", "340282366920938463463374607431768211455"],
        ["date_epoch.bin", "1970-01-01"],
        ["datetime.bin", "2023-11-14T22:13:20.000000000Z"],
        ["time_noon.bin", "12:00:00.000000000"],
        ["duration.bin", "1mo2d3ns"],
        ["identity_id.bin", "00000000-0000-0000-0000-000000000007"],
        ["uuid4_nil.bin", "00000000-0000-0000-0000-000000000000"],
        ["uuid7.bin", "00000000-0000-0000-0123-456789abcdef"],
        ["blob.bin", "0x00ff7f"],
        ["int_big_negative.bin", "-12345678901234567890"],
        ["uint_big.bin", "98765432109876543210"],
        ["decimal_pi.bin", "3.14159"],
        ["any_int4.bin", "5"],
        ["any_none_duration.bin", NONE_VALUE],
        ["dictionary_id_u2.bin", "300"],
        ["type_option_int4.bin", "Option(Int4)"],
        ["list_mixed.bin", `[1, two, ${NONE_VALUE}]`],
        ["record.bin", "{k: false}"],
        ["tuple.bin", "(1, 2)"],
    ];

    it("covers every value/*.bin fixture", () => {
        const fixtures = fs.readdirSync(path.join(goldenRoot, "value")).filter((f) => f.endsWith(".bin")).sort();
        expect(valueCases.map(([f]) => f).sort()).toEqual(fixtures);
    });

    for (const [fixture, expected] of valueCases) {
        it(`decodes value/${fixture}`, () => {
            const bytes = new Uint8Array(fs.readFileSync(path.join(goldenRoot, "value", fixture)));
            const { value, next_pos } = decode_any_value(bytes, 0);
            expect(value).toBe(expected);
            // The value must consume the fixture exactly (no trailing bytes),
            // which pins the payload widths and the recursive typeinfo framing.
            expect(next_pos).toBe(bytes.length);
        });
    }
});
