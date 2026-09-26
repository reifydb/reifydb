// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from "vitest";
import { DigestValue, NONE_VALUE, NoneValue, columnsToRows, type DigestType } from "@reifydb/core";
import { rbcf, type WireFrame } from "../src/rbcf";
import { TYPE_CODE } from "../src/rbcf/format";
import { decodeTypeInfo } from "../src/rbcf/typeinfo";
import { decodeAnyValue } from "../src/rbcf/encoding/plain";
import { decodeJsonResponse } from "../src/json-decode";

const FLOAT8: DigestType = { Digest: { inner: "Float8", accuracy: 10_000 } };
const DURATION: DigestType = { Digest: { inner: "Duration", accuracy: 100_000 } };

const SAMPLE = [
    0x01, 0x03, 0x90, 0x4e, 0x01, 0x00, 0x01, 0x01, 0x46, 0x01, 0x03, 0x43, 0x01, 0x22, 0x02, 0x23, 0x01,
];
const SAMPLE_HEX = "0x0103904e01000101460103430122022301";
const EMPTY_HEX = "0x0103904e0000000000";
const DURATION_MS_HEX = "0x0112a08d0600000000039a010104011001";
const DURATION_DAYS_HEX = "0x0112a08d060000000002c202010301";

const RUST_FRAME = [
    0x52, 0x42, 0x43, 0x46, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x6c, 0x00, 0x00, 0x00,
    0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x5c, 0x00, 0x00, 0x00,
    0x5e, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x1a, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
    0x64, 0x00, 0x00, 0x00,
    0x05,
    0x01, 0x03, 0x90, 0x4e, 0x01, 0x00, 0x01, 0x01, 0x46, 0x01, 0x03, 0x43, 0x01, 0x22, 0x02, 0x23, 0x01,
    0x01, 0x03, 0x90, 0x4e, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00,
    0x03, 0x10, 0x27, 0x00, 0x00,
];
const COLUMN_AT = 28;
const DATA_AT = 61;
const OFFSETS_AT = 87;
const EXTRA_AT = 103;

const RUST_FRAME_COLUMNS: WireFrame[] = [
    { columns: [{ name: "d", type: { Option: FLOAT8 }, payload: [SAMPLE_HEX, NONE_VALUE, EMPTY_HEX] }] },
];

function patched(at: number, bytes: number[]): Uint8Array {
    const out = new Uint8Array(RUST_FRAME);
    out.set(bytes, at);
    return out;
}

describe("RBCF digest column", () => {
    it("decodes the bytes the Rust encoder writes for an option digest column", () => {
        // Decode must read exactly the type code, extra section, offsets and none bitmap that Rust writes.
        const frames = rbcf.decode(new Uint8Array(RUST_FRAME));
        expect(frames).toEqual(RUST_FRAME_COLUMNS);

        const rows = columnsToRows(frames[0].columns);
        expect(rows[0].d).toBeInstanceOf(DigestValue);
        expect((rows[0].d as DigestValue).count).toBe(7n);
        expect(rows[0].d.toString()).toBe("digest(n: 7)");
        expect(rows[1].d).toBeInstanceOf(NoneValue);
        expect((rows[1].d as NoneValue).innerType).toEqual(FLOAT8);
        expect((rows[2].d as DigestValue).count).toBe(0n);
    });

    it("encodes exactly the bytes the Rust encoder writes", () => {
        // The server decodes what the client sends, so the TypeScript encoder must match Rust byte for byte.
        expect(rbcf.encode(RUST_FRAME_COLUMNS)).toEqual(new Uint8Array(RUST_FRAME));
    });

    it("round trips a duration digest column with a day part and no nones", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "latency", type: DURATION, payload: [DURATION_MS_HEX, DURATION_DAYS_HEX] }] },
        ];
        const decoded = rbcf.decode(rbcf.encode(frames));
        expect(decoded).toEqual(frames);
        expect(columnsToRows(decoded[0].columns).map((row) => row.latency.toString()))
            .toEqual(["digest(n: 3)", "digest(n: 2)"]);
    });

    it("rejects a row digest whose accuracy differs from the column", () => {
        // A row digest with another accuracy would merge wrongly later, so decode must refuse it.
        const bytes = patched(EXTRA_AT + 1, [0x20, 0x4e, 0x00, 0x00]);
        expect(() => rbcf.decode(bytes))
            .toThrow("digest in row 0 is Digest(Float8, 0.01) but the column is Digest(Float8, 0.02)");
    });

    it("rejects column params that are not a valid digest type", () => {
        expect(() => rbcf.decode(patched(EXTRA_AT, [TYPE_CODE.Utf8]))).toThrow("digest does not support Utf8 input");
        expect(() => rbcf.decode(patched(EXTRA_AT + 1, [0xe7, 0x03, 0x00, 0x00])))
            .toThrow("digest accuracy must be a whole number of ppm");
    });

    it("rejects a digest column that is not plain encoded", () => {
        // A digest must never be read from an encoding without the extra section and row spans.
        expect(() => rbcf.decode(patched(COLUMN_AT + 1, [2]))).toThrow("digest column must use plain encoding, found Rle");
    });

    it("rejects malformed row digest bytes instead of passing them on", () => {
        expect(() => rbcf.decode(patched(DATA_AT, [0x02])))
            .toThrow("invalid digest in row 0: digest encoding version 2 is unknown");
    });

    it("rejects a row span outside the data section", () => {
        expect(() => rbcf.decode(patched(OFFSETS_AT + 12, [0x1b]))).toThrow("digest row 2 spans 17..27 outside 26 data bytes");
    });

    it("refuses to encode a present row that is not a digest of the column type", () => {
        // An empty or mismatched present row would otherwise be written as a none or a wrong digest.
        const column = (payload: string[]): WireFrame[] => [{ columns: [{ name: "d", type: FLOAT8, payload }] }];
        expect(() => rbcf.encode(column([""]))).toThrow("as Digest(Float8, 0.01)");
        expect(() => rbcf.encode(column([DURATION_MS_HEX]))).toThrow("but the type is Digest(Float8, 0.01)");
        expect(() => rbcf.encode(column(["0x01"]))).toThrow("digest encoding ends early");
    });
});

describe("RBCF digest typeinfo", () => {
    it("reads the typeinfo bytes pinned by the Rust codec", () => {
        expect(decodeTypeInfo(new Uint8Array([32, 18, 0x39, 0x30, 0, 0]), 0))
            .toEqual({ name: "Digest(Duration, 0.012345)", nextPos: 6 });
        expect(decodeTypeInfo(new Uint8Array([(1 << 6) | 32, 3, 0x10, 0x27, 0, 0]), 0))
            .toEqual({ name: "Option(Digest(Float8, 0.01))", nextPos: 6 });
    });

    it("rejects an unsupported inner type, an accuracy out of range and truncated params", () => {
        expect(() => decodeTypeInfo(new Uint8Array([32, 9, 0x10, 0x27, 0, 0]), 0)).toThrow("digest does not support Utf8 input");
        expect(() => decodeTypeInfo(new Uint8Array([32, 3, 0xe7, 0x03, 0, 0]), 0)).toThrow("whole number of ppm");
        expect(() => decodeTypeInfo(new Uint8Array([32, 3, 0x10]), 0)).toThrow("RBCF: digest params truncated");
    });
});

describe("RBCF digest inside an Any value", () => {
    it("renders a length-prefixed digest as its count", () => {
        // The read must stop exactly at the u32 length, never at the end of the buffer.
        const bytes = new Uint8Array([TYPE_CODE.Digest, SAMPLE.length, 0, 0, 0, ...SAMPLE, 0xaa]);
        expect(decodeAnyValue(bytes, 0)).toEqual({ value: "digest(n: 7)", nextPos: 5 + SAMPLE.length });
    });

    it("rejects a length that runs past the data", () => {
        const bytes = new Uint8Array([TYPE_CODE.Digest, SAMPLE.length + 1, 0, 0, 0, ...SAMPLE]);
        expect(() => decodeAnyValue(bytes, 0)).toThrow("RBCF: digest value truncated");
    });
});

describe("JSON digest response", () => {
    it("decodes digest rows through the envelope types", () => {
        const response = [{
            types: { d: { id: "Option", underlying: { id: "Digest", underlying: { id: "Float8" }, accuracy: 10000 } } },
            rows: [{ d: SAMPLE_HEX }, { d: null }],
        }];
        const [rows] = decodeJsonResponse(response);
        expect(rows[0].d).toBeInstanceOf(DigestValue);
        expect(rows[0].d.toString()).toBe("digest(n: 7)");
        expect(rows[1].d).toBeInstanceOf(NoneValue);
    });
});
