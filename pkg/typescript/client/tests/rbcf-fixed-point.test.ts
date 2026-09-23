// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from "vitest";
import { DecimalValue, NONE_VALUE, columnsToRows } from "@reifydb/core";
import { rbcf } from "../src/rbcf";
import { ColumnEncoding, TYPE_CODE } from "../src/rbcf/format";
import { decodeTypeInfo } from "../src/rbcf/typeinfo";
import { decodeAnyValue } from "../src/rbcf/encoding/plain";

const MAX76 = 10n ** 76n - 1n;
const MAX38 = 10n ** 38n - 1n;

function le(value: bigint, width: number): number[] {
    const out: number[] = [];
    let v = BigInt.asUintN(width * 8, value);
    for (let i = 0; i < width; i++) {
        out.push(Number(v & 0xffn));
        v >>= 8n;
    }
    return out;
}

function u32(v: number): number[] {
    return le(BigInt(v), 4);
}

interface ColumnSpec {
    kind: number;
    encoding: ColumnEncoding;
    rowCount: number;
    data: number[];
    extra: number[];
    nones?: number[];
}

function message(spec: ColumnSpec): Uint8Array {
    const depth = spec.nones ? 1 : 0;
    const nones = spec.nones ?? [];
    const column = [
        (depth << 6) | spec.kind, spec.encoding, depth, 0,
        1, 0, 0, 0,
        ...u32(spec.rowCount), ...u32(nones.length), ...u32(spec.data.length), ...u32(0), ...u32(spec.extra.length),
        0x76, 0, 0, 0,
        ...nones, ...spec.data, ...spec.extra,
    ];
    const frame = [...u32(spec.rowCount), 1, 0, 0, 0, ...u32(12 + column.length), ...column];
    const bytes = [0x52, 0x42, 0x43, 0x46, 1, 0, 0, 0, ...u32(1), ...u32(16 + frame.length), ...frame];
    return new Uint8Array(bytes);
}

function decodeColumn(spec: ColumnSpec) {
    return rbcf.decode(message(spec))[0].columns[0];
}

describe("RBCF fixed-point columns", () => {
    it("reads a precision 38 decimal from 16 byte slots and renders it at the column scale", () => {
        // Precision 38 is the last width that fits 16 bytes; reading 32 would shift every later row.
        const col = decodeColumn({
            kind: TYPE_CODE.Decimal,
            encoding: ColumnEncoding.Plain,
            rowCount: 3,
            data: [...le(-123_456_000_000n, 16), ...le(1n, 16), ...le(MAX38, 16)],
            extra: [38, 9],
        });
        expect(col.type).toEqual({ Decimal: { precision: 38, scale: 9 } });
        expect(col.payload).toEqual(["-123.456000000", "0.000000001", "99999999999999999999999999999.999999999"]);
    });

    it("reads a precision 39 int from 32 byte slots across the full 256 bit sign range", () => {
        // Precision 39 must read 32 byte slots, otherwise the sign in the last byte is lost.
        const col = decodeColumn({
            kind: TYPE_CODE.Int,
            encoding: ColumnEncoding.Plain,
            rowCount: 3,
            data: [...le(-(10n ** 39n - 1n), 32), ...le(0n, 32), ...le(10n ** 39n - 1n, 32)],
            extra: [39, 0],
        });
        expect(col.type).toEqual({ Int: { precision: 39 } });
        expect(col.payload).toEqual([`-${10n ** 39n - 1n}`, "0", `${10n ** 39n - 1n}`]);
    });

    it("reads a default uint at 76 digits and keeps a none row as the none marker", () => {
        // A none row still occupies a slot; skipping it would misalign the defined rows after it.
        const col = decodeColumn({
            kind: TYPE_CODE.Uint,
            encoding: ColumnEncoding.Plain,
            rowCount: 3,
            data: [...le(MAX76, 32), ...le(0n, 32), ...le(7n, 32)],
            extra: [76, 0],
            nones: [0b101],
        });
        expect(col.type).toEqual({ Option: { Uint: { precision: 76 } } });
        expect(col.payload).toEqual([`${MAX76}`, NONE_VALUE, "7"]);
    });

    it("reads RLE runs of 32 byte values", () => {
        const col = decodeColumn({
            kind: TYPE_CODE.Decimal,
            encoding: ColumnEncoding.Rle,
            rowCount: 5,
            data: [...le(-MAX76, 32), ...u32(2), ...le(15n, 32), ...u32(3)],
            extra: [76, 10],
        });
        expect(col.type).toEqual({ Decimal: { precision: 76, scale: 10 } });
        const min = `-${"9".repeat(66)}.${"9".repeat(10)}`;
        expect(col.payload).toEqual([min, min, "0.0000000015", "0.0000000015", "0.0000000015"]);
    });

    it("reads RLE runs of 16 byte values", () => {
        const col = decodeColumn({
            kind: TYPE_CODE.Int,
            encoding: ColumnEncoding.Rle,
            rowCount: 3,
            data: [...le(-5n, 16), ...u32(1), ...le(MAX38, 16), ...u32(2)],
            extra: [38, 0],
        });
        expect(col.payload).toEqual(["-5", `${MAX38}`, `${MAX38}`]);
    });

    it("reads delta with an i128 baseline for a narrow column", () => {
        // A narrow column must use the int16 delta layout with a 16 byte baseline, never 32.
        const col = decodeColumn({
            kind: TYPE_CODE.Int,
            encoding: ColumnEncoding.Delta,
            rowCount: 4,
            data: [1, ...le(-MAX38, 16), 10, 0xf6, 5],
            extra: [38, 0],
        });
        expect(col.payload).toEqual([`${-MAX38}`, `${-MAX38 + 10n}`, `${-MAX38}`, `${-MAX38 + 5n}`]);
    });

    it("reads delta with an i256 baseline for a wide column", () => {
        // A wide column keeps a 32 byte baseline; reading 16 would lose the high half of the value.
        const col = decodeColumn({
            kind: TYPE_CODE.Decimal,
            encoding: ColumnEncoding.Delta,
            rowCount: 3,
            data: [2, ...le(MAX76 - 1000n, 32), ...le(500n, 2), ...le(500n, 2)],
            extra: [76, 2],
        });
        expect(col.payload).toEqual([
            `${"9".repeat(72)}89.99`,
            `${"9".repeat(72)}94.99`,
            `${"9".repeat(74)}.99`,
        ]);
    });

    it("reads delta rle with an i256 baseline", () => {
        const col = decodeColumn({
            kind: TYPE_CODE.Uint,
            encoding: ColumnEncoding.DeltaRle,
            rowCount: 4,
            data: [16, ...le(10n ** 60n, 32), ...le(10n ** 30n, 16), ...u32(3)],
            extra: [61, 0],
        });
        expect(col.payload).toEqual([
            `${10n ** 60n}`,
            `${10n ** 60n + 10n ** 30n}`,
            `${10n ** 60n + 2n * 10n ** 30n}`,
            `${10n ** 60n + 3n * 10n ** 30n}`,
        ]);
    });

    it("decodes an empty column", () => {
        const col = decodeColumn({ kind: TYPE_CODE.Decimal, encoding: ColumnEncoding.Delta, rowCount: 0, data: [], extra: [5, 5] });
        expect(col).toEqual({ name: "v", type: { Decimal: { precision: 5, scale: 5 } }, payload: [] });
    });

    it("hands a decimal column to core as DecimalValue rows", () => {
        const col = decodeColumn({
            kind: TYPE_CODE.Decimal,
            encoding: ColumnEncoding.Plain,
            rowCount: 1,
            data: le(-5n, 16),
            extra: [10, 3],
        });
        const rows = columnsToRows([col]);
        expect(rows[0].v).toBeInstanceOf(DecimalValue);
        expect(rows[0].v.toString()).toBe("-0.005");
    });

    it.each([
        ["no extra bytes", { extra: [] }, "precision and scale in 2 extra bytes"],
        ["three extra bytes", { extra: [10, 0, 0] }, "precision and scale in 2 extra bytes"],
        ["precision 0", { extra: [0, 0] }, "precision"],
        ["precision 77", { extra: [77, 0] }, "precision"],
        ["int with a scale", { extra: [10, 1] }, "scale 0"],
        ["dict encoding", { encoding: ColumnEncoding.Dict }, "Dict encoding not supported"],
        ["bitpack encoding", { encoding: ColumnEncoding.BitPack }, "BitPack encoding not supported"],
        ["more digits than the precision", { data: le(1000n, 16), extra: [3, 0] }, "more digits than precision 3"],
    ])("rejects an int column with %s", (_, override, message) => {
        // A decoder that accepts these would render values the server never wrote.
        const spec: ColumnSpec = {
            kind: TYPE_CODE.Int,
            encoding: ColumnEncoding.Plain,
            rowCount: 1,
            data: le(1n, 16),
            extra: [10, 0],
            ...override,
        };
        expect(() => decodeColumn(spec)).toThrow(message);
    });

    it("rejects a decimal whose scale exceeds its precision", () => {
        expect(() =>
            decodeColumn({ kind: TYPE_CODE.Decimal, encoding: ColumnEncoding.Plain, rowCount: 1, data: le(1n, 16), extra: [5, 6] })
        ).toThrow("scale");
    });

    it("rejects a negative uint", () => {
        // A set sign bit on a uint must be rejected as corrupt, never read as a large number.
        expect(() =>
            decodeColumn({ kind: TYPE_CODE.Uint, encoding: ColumnEncoding.Plain, rowCount: 1, data: le(-1n, 16), extra: [10, 0] })
        ).toThrow("negative");
    });

    it("rejects plain data shorter than the rows need", () => {
        expect(() =>
            decodeColumn({ kind: TYPE_CODE.Int, encoding: ColumnEncoding.Plain, rowCount: 2, data: le(1n, 32), extra: [76, 0] })
        ).toThrow("do not hold 2 values of 32 bytes");
    });
});

describe("RBCF fixed-point typeinfo", () => {
    it.each([
        [[TYPE_CODE.Int, 76], "Int"],
        [[TYPE_CODE.Int, 38], "Int(38)"],
        [[TYPE_CODE.Uint, 1], "Uint(1)"],
        [[TYPE_CODE.Decimal, 76, 10], "Decimal"],
        [[TYPE_CODE.Decimal, 38, 9], "Decimal(38, 9)"],
        [[TYPE_CODE.Decimal, 76, 0], "Decimal(76, 0)"],
        [[(1 << 6) | TYPE_CODE.Decimal, 12, 2], "Option(Decimal(12, 2))"],
    ])("reads %j as %s and consumes the parameter bytes", (bytes, name) => {
        // The parameters are inline, so a reader that skips them misreads every following type.
        expect(decodeTypeInfo(new Uint8Array(bytes), 0)).toEqual({ name, nextPos: bytes.length });
    });

    it("rejects typeinfo cut off before its parameters", () => {
        expect(() => decodeTypeInfo(new Uint8Array([TYPE_CODE.Decimal, 38]), 0)).toThrow("truncated");
        expect(() => decodeTypeInfo(new Uint8Array([TYPE_CODE.Int]), 0)).toThrow("truncated");
    });
});

describe("RBCF fixed-point Any values", () => {
    it("reads int and uint as 32 byte values", () => {
        const int = [TYPE_CODE.Int, ...le(-MAX76, 32)];
        expect(decodeAnyValue(new Uint8Array(int), 0)).toEqual({ value: `${-MAX76}`, nextPos: 33 });
        const uint = [TYPE_CODE.Uint, ...le(MAX76, 32)];
        expect(decodeAnyValue(new Uint8Array(uint), 0)).toEqual({ value: `${MAX76}`, nextPos: 33 });
    });

    it("reads a decimal as a scale byte and a 32 byte unscaled value", () => {
        const bytes = [TYPE_CODE.Decimal, 3, ...le(-1500n, 32)];
        expect(decodeAnyValue(new Uint8Array(bytes), 0)).toEqual({ value: "-1.500", nextPos: 34 });
    });

    it("rejects a negative uint and a value beyond 76 digits", () => {
        expect(() => decodeAnyValue(new Uint8Array([TYPE_CODE.Uint, ...le(-1n, 32)]), 0)).toThrow("negative");
        expect(() => decodeAnyValue(new Uint8Array([TYPE_CODE.Int, ...le(MAX76 + 1n, 32)]), 0)).toThrow("76 digits");
    });

    it("rejects a truncated value", () => {
        expect(() => decodeAnyValue(new Uint8Array([TYPE_CODE.Int, ...le(1n, 16)]), 0)).toThrow("truncated");
    });
});
