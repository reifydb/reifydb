// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {
    BlobValue, DigestType, DigestValue, NoneValue, Option, decode, columnsToRows, digestType, digestTypeName,
    encodeParams, encodeValue, envelopeToColumns, noneMarker, typeFromWire, typeToWire,
} from '../../src';

const U64_MAX = 18446744073709551615n;
const U32_MAX = 4294967295n;
const I32_MAX = 2147483647n;
const I32_MIN = -2147483648n;

const SAMPLE = [
    0x01, 0x03, 0x90, 0x4e, 0x01, 0x00, 0x01, 0x01, 0x46, 0x01, 0x03, 0x43, 0x01, 0x22, 0x02, 0x23, 0x01,
];
const SAMPLE_HEX = '0x0103904e01000101460103430122022301';
const EMPTY_FLOAT8 = [1n, 3n, 10_000n, 0n, 0n, 0n, 0n, 0n];
const FLOAT8: DigestType = {Digest: {inner: 'Float8', accuracy: 10_000}};

function varints(values: bigint[]): number[] {
    const out: number[] = [];
    for (let value of values) {
        while (value >= 0x80n) {
            out.push(Number(value & 0x7fn) | 0x80);
            value >>= 7n;
        }
        out.push(Number(value));
    }
    return out;
}

function zigzag(index: bigint): bigint {
    return index >= 0n ? index * 2n : -index * 2n - 1n;
}

function headerWith(position: number, value: bigint): bigint[] {
    const fields = EMPTY_FLOAT8.slice(0, 6);
    fields[position] = value;
    return fields;
}

function withPositive(positive: bigint[]): Uint8Array {
    return new Uint8Array(varints([...EMPTY_FLOAT8.slice(0, 7), ...positive]));
}

function withNegative(negative: bigint[]): Uint8Array {
    return new Uint8Array(varints([...EMPTY_FLOAT8.slice(0, 6), ...negative, 0n]));
}

function bytes(values: number[]): Uint8Array {
    return new Uint8Array(values);
}

describe('digest test helpers', () => {
    it('build varints and zigzag indexes byte for byte like the Rust encoder', () => {
        // The malformed cases below are built with these helpers, so a drift would test the wrong bytes.
        expect(varints([0n])).toEqual([0x00]);
        expect(varints([127n])).toEqual([0x7f]);
        expect(varints([128n])).toEqual([0x80, 0x01]);
        expect(varints([300n])).toEqual([0xac, 0x02]);
        expect(varints([U64_MAX])).toEqual([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
        expect(zigzag(0n)).toBe(0n);
        expect(zigzag(-1n)).toBe(1n);
        expect(zigzag(1n)).toBe(2n);
        expect(zigzag(-34n)).toBe(67n);
        expect(zigzag(I32_MAX)).toBe(U32_MAX - 1n);
        expect(zigzag(I32_MIN)).toBe(U32_MAX);
    });
});

describe('DigestValue', () => {
    it('reads the pinned canonical bytes of the Rust sample digest', () => {
        // Every store and special count must add up: -2, 0, 0.5, 1, 1, 2 and +inf give exactly 7.
        const digest = new DigestValue(bytes(SAMPLE));
        expect(digest.count).toBe(7n);
        expect(digest.inner).toBe('Float8');
        expect(digest.accuracy).toBe(10_000);
        expect(digest.type).toEqual(FLOAT8);
        expect(digest.asBytes()).toEqual(bytes(SAMPLE));
        expect(digest.toString()).toBe('digest(n: 7)');
    });

    it('reads an empty digest as count 0', () => {
        const digest = new DigestValue(new Uint8Array(varints(EMPTY_FLOAT8)));
        expect(digest.count).toBe(0n);
        expect(digest.toString()).toBe('digest(n: 0)');
    });

    it('maps every frozen inner type tag to its type name', () => {
        // Tags are stored type numbers, otherwise a Duration digest reads back as another type.
        const pinned: Array<[string, bigint]> = [
            ['Float4', 2n], ['Float8', 3n], ['Int1', 4n], ['Int2', 5n], ['Int4', 6n], ['Int8', 7n], ['Int16', 8n],
            ['Uint1', 10n], ['Uint2', 11n], ['Uint4', 12n], ['Uint8', 13n], ['Uint16', 14n], ['Duration', 18n],
        ];
        for (const [inner, tag] of pinned) {
            const digest = new DigestValue(new Uint8Array(varints([1n, tag, 10_000n, 0n, 0n, 0n, 0n, 0n])));
            expect(digest.inner, inner).toBe(inner);
        }
    });

    it('keeps a count above 2^53 exact', () => {
        // Without bigint a u64 count rounds and prints a different n than the server.
        const largest = new DigestValue(new Uint8Array(varints([...headerWith(3, U64_MAX), 0n, 0n])));
        expect(largest.count).toBe(U64_MAX);
        expect(largest.toString()).toBe('digest(n: 18446744073709551615)');
    });

    it('accepts a zero count in a duration digest', () => {
        const digest = new DigestValue(new Uint8Array(varints([1n, 18n, 10_000n, 1n, 0n, 0n, 0n, 0n])));
        expect(digest.inner).toBe('Duration');
        expect(digest.count).toBe(1n);
    });

    it('copies the bytes in and out', () => {
        // Without copies a caller could change a digest after it was decoded.
        const input = bytes(SAMPLE);
        const digest = new DigestValue(input);
        input[1] = 18;
        const out = digest.asBytes();
        out[1] = 18;
        expect(digest.asBytes()).toEqual(bytes(SAMPLE));
        expect(digest.inner).toBe('Float8');
    });

    it('rejects an unknown version', () => {
        for (const version of [0n, 2n, 1n << 40n]) {
            const input = new Uint8Array(varints([...headerWith(0, version), 0n, 0n]));
            expect(() => new DigestValue(input)).toThrow(`digest encoding version ${version} is unknown`);
        }
    });

    it('rejects trailing bytes', () => {
        expect(() => new DigestValue(bytes([...SAMPLE, 0x00]))).toThrow('digest encoding has 1 trailing bytes');
    });

    it('rejects every truncation', () => {
        for (let len = 0; len < SAMPLE.length; len++) {
            expect(() => new DigestValue(bytes(SAMPLE.slice(0, len))), `prefix of ${len} bytes`)
                .toThrow('digest encoding ends early');
        }
        expect(() => new DigestValue(bytes([0x01, 0x83]))).toThrow('digest encoding ends early');
    });

    it('rejects a repeated bucket index', () => {
        expect(() => new DigestValue(withPositive([2n, 0n, 1n, 0n, 1n])))
            .toThrow('digest encoding repeats bucket index 0');
        expect(() => new DigestValue(withNegative([3n, 9n, 1n, 1n, 1n, 0n, 1n])))
            .toThrow('digest encoding repeats bucket index -4');
    });

    it('rejects bucket indexes that leave i32', () => {
        // Deltas are unsigned, so an unsorted store must show up as an index past i32.
        const outside = 'digest encoding has a bucket index outside i32';
        expect(() => new DigestValue(withPositive([2n, zigzag(I32_MAX), 1n, 1n, 1n]))).toThrow(outside);
        expect(() => new DigestValue(withPositive([2n, zigzag(I32_MIN), 1n, U64_MAX, 1n]))).toThrow(outside);
        expect(() => new DigestValue(withPositive([1n, 1n << 32n, 1n]))).toThrow(outside);
        const widest = new DigestValue(withPositive([2n, zigzag(I32_MIN), 1n, U32_MAX, 1n]));
        expect(widest.count).toBe(2n);
    });

    it('rejects a zero bucket count', () => {
        expect(() => new DigestValue(withPositive([1n, 0n, 0n])))
            .toThrow('digest encoding has a zero count at bucket 0');
        expect(() => new DigestValue(withNegative([2n, 9n, 1n, 3n, 0n])))
            .toThrow('digest encoding has a zero count at bucket -2');
    });

    it('rejects non-minimal varints', () => {
        const paddedVersion = [0x81, 0x00, ...varints([3n, 10_000n, 0n, 0n, 0n, 0n, 0n])];
        const paddedAccuracy = [...varints([1n, 3n]), 0x90, 0xce, 0x00, ...varints([0n, 0n, 0n, 0n, 0n])];
        const paddedCount = [...varints(EMPTY_FLOAT8.slice(0, 7)), 0x01, 0x00, 0x81, 0x00];
        const paddedDelta = [...varints(EMPTY_FLOAT8.slice(0, 7)), 0x02, 0x00, 0x01, 0x81, 0x00, 0x01];
        for (const input of [paddedVersion, paddedAccuracy, paddedCount, paddedDelta]) {
            expect(() => new DigestValue(bytes(input)), JSON.stringify(input))
                .toThrow('digest encoding has a varint that is not minimal');
        }
    });

    it('rejects varints above u64', () => {
        const fields = varints(EMPTY_FLOAT8.slice(0, 3));
        const tenContinued = [...fields, ...new Array(10).fill(0xff), 0x01];
        const tenthTooHigh = [...fields, ...new Array(9).fill(0xff), 0x02];
        for (const input of [tenContinued, tenthTooHigh]) {
            expect(() => new DigestValue(bytes(input))).toThrow('digest encoding has a varint above u64');
        }
    });

    it('rejects an unknown inner type tag', () => {
        for (const tag of [0n, 1n, 9n, 15n, 25n, 32n, 63n, 255n]) {
            const input = new Uint8Array(varints([...headerWith(1, tag), 0n, 0n]));
            expect(() => new DigestValue(input)).toThrow(`digest encoding has unknown inner type tag ${tag}`);
        }
    });

    it('rejects an accuracy outside 1000 to 100000 ppm', () => {
        for (const accuracy of [0n, 999n, 100_001n, 1n << 32n]) {
            const input = new Uint8Array(varints([...headerWith(2, accuracy), 0n, 0n]));
            expect(() => new DigestValue(input), `${accuracy}`).toThrow('digest accuracy must be a whole number of ppm');
        }
    });

    it('rejects counts that overflow u64', () => {
        const specials = new Uint8Array(varints([1n, 3n, 10_000n, U64_MAX, 0n, 1n, 0n, 0n]));
        const buckets = withPositive([2n, 0n, U64_MAX, 1n, 1n]);
        for (const input of [specials, buckets]) {
            expect(() => new DigestValue(input)).toThrow('digest encoding counts overflow u64');
        }
    });

    it('rejects infinity in a duration digest', () => {
        for (const position of [4, 5]) {
            const fields = headerWith(position, 1n);
            fields[1] = 18n;
            expect(() => new DigestValue(new Uint8Array(varints([...fields, 0n, 0n]))), `${position}`)
                .toThrow('digest of duration cannot hold infinity');
        }
    });

    it('compares by canonical bytes', () => {
        // Equal digests must have equal bytes, so a byte compare is exact equality.
        expect(new DigestValue(bytes(SAMPLE)).equals(new DigestValue(bytes(SAMPLE)))).toBe(true);
        expect(new DigestValue(bytes(SAMPLE)).equals(new DigestValue(new Uint8Array(varints(EMPTY_FLOAT8))))).toBe(false);
        expect(new DigestValue(bytes(SAMPLE)).equals(new BlobValue(bytes(SAMPLE)))).toBe(false);
    });

    it('serializes to JSON as the wire payload', () => {
        expect(JSON.stringify({d: new DigestValue(bytes(SAMPLE))})).toBe(`{"d":"${SAMPLE_HEX}"}`);
    });
});

describe('DigestValue.parse', () => {
    it('reads the 0x hex payload of the JSON wire', () => {
        const digest = DigestValue.parse(SAMPLE_HEX, FLOAT8);
        expect(digest.asBytes()).toEqual(bytes(SAMPLE));
        expect(DigestValue.parse(SAMPLE_HEX.toUpperCase().replace('0X', '0x'), FLOAT8).count).toBe(7n);
    });

    it('rejects a digest whose inner type or accuracy differs from the column type', () => {
        // A digest of another accuracy would merge wrongly later, so the column type must match exactly.
        expect(() => DigestValue.parse(SAMPLE_HEX, {Digest: {inner: 'Float8', accuracy: 20_000}}))
            .toThrow('digest is Digest(Float8, 0.01) but the type is Digest(Float8, 0.02)');
        expect(() => DigestValue.parse(SAMPLE_HEX, {Digest: {inner: 'Duration', accuracy: 10_000}}))
            .toThrow('digest is Digest(Float8, 0.01) but the type is Digest(Duration, 0.01)');
    });

    it('rejects text that is not 0x hex instead of reading it as an empty digest', () => {
        for (const text of ['', 'none', '0103904e0000000000', '0x0', '0xzz', `0x${'0'.repeat(3)}`]) {
            expect(() => DigestValue.parse(text, FLOAT8), text).toThrow(`as Digest(Float8, 0.01)`);
        }
        expect(() => DigestValue.parse('0x', FLOAT8)).toThrow('digest encoding ends early');
    });
});

describe('digest type', () => {
    it('renders like the Rust type display', () => {
        expect(digestTypeName({Digest: {inner: 'Float8', accuracy: 10_000}})).toBe('Digest(Float8, 0.01)');
        expect(digestTypeName({Digest: {inner: 'Duration', accuracy: 12_345}})).toBe('Digest(Duration, 0.012345)');
        expect(digestTypeName({Digest: {inner: 'Int4', accuracy: 1_000}})).toBe('Digest(Int4, 0.001)');
    });

    it('rejects an inner type or accuracy a digest cannot have', () => {
        expect(() => digestType('Utf8', 10_000)).toThrow('digest does not support Utf8 input');
        expect(() => digestType('Option', 10_000)).toThrow('digest does not support Option input');
        for (const accuracy of [999, 100_001, 10_000.5, NaN]) {
            expect(() => digestType('Float8', accuracy), `${accuracy}`).toThrow('digest accuracy must be a whole number of ppm');
        }
        expect(digestType('Float8', 10_000)).toEqual(FLOAT8);
    });

    it('names a digest on the JSON wire with its inner type and accuracy', () => {
        // The server reads exactly this descriptor; without accuracy it cannot rebuild the type.
        const duration: DigestType = {Digest: {inner: 'Duration', accuracy: 10_000}};
        expect(typeToWire(duration)).toEqual({id: 'Digest', underlying: {id: 'Duration'}, accuracy: 10000});
        expect(typeFromWire({id: 'Digest', underlying: {id: 'Duration'}, accuracy: 10000})).toEqual(duration);
        const option = {Option: {Digest: {inner: 'Int4', accuracy: 1_000}}} as const;
        expect(typeFromWire(typeToWire(option))).toEqual(option);
    });

    it('rejects a digest wire descriptor without valid params', () => {
        expect(() => typeFromWire({id: 'Digest', underlying: {id: 'Float8'}})).toThrow('numeric accuracy');
        expect(() => typeFromWire({id: 'Digest', accuracy: 10000})).toThrow('underlying type');
        expect(() => typeFromWire({id: 'Digest', underlying: {id: 'Float8'}, accuracy: -1})).toThrow('whole number of ppm');
        expect(() => typeFromWire({id: 'Digest', underlying: {id: 'Float8'}, accuracy: 5_000_000_000})).toThrow('whole number of ppm');
        expect(() => typeFromWire({id: 'Digest', underlying: {id: 'Utf8'}, accuracy: 10000})).toThrow('does not support Utf8');
        expect(() => typeFromWire({id: 'Digest', underlying: {id: 'Float8'}, accuracy: 500_000})).toThrow('whole number of ppm');
    });
});

describe('digest decode and encode', () => {
    it('decodes a digest payload and a typed none under an option', () => {
        const value = decode({type: FLOAT8, value: SAMPLE_HEX});
        expect(value).toBeInstanceOf(DigestValue);
        expect(value.toString()).toBe('digest(n: 7)');

        const none = decode({type: {Option: FLOAT8}, value: noneMarker(0)});
        expect(none).toBeInstanceOf(NoneValue);
        expect((none as NoneValue).innerType).toEqual(FLOAT8);
    });

    it('rejects a none payload in a digest column that is not an option', () => {
        // Other legacy types turn this into an empty value; a digest must never pretend to hold count 0.
        expect(() => decode({type: FLOAT8, value: noneMarker(0)})).toThrow('as Digest(Float8, 0.01)');
    });

    it('decodes the rows of a JSON envelope', () => {
        const envelope = {
            types: {d: {id: 'Option', underlying: {id: 'Digest', underlying: {id: 'Float8'}, accuracy: 10000}}},
            rows: [{d: SAMPLE_HEX}, {d: null}],
        };
        const rows = columnsToRows(envelopeToColumns(envelope));
        expect(rows[0].d).toBeInstanceOf(DigestValue);
        expect((rows[0].d as DigestValue).count).toBe(7n);
        expect(rows[1].d).toBeInstanceOf(NoneValue);
        expect((rows[1].d as NoneValue).innerType).toEqual(FLOAT8);
    });

    it('encodes a digest parameter so it round trips through decode', () => {
        const digest = new DigestValue(bytes(SAMPLE));
        const pair = encodeValue(digest);
        expect(pair).toEqual({type: FLOAT8, value: SAMPLE_HEX});
        expect(decode(pair).equals(digest)).toBe(true);
    });

    it('sends a digest parameter with the wire descriptor the server reads', () => {
        const digest = new DigestValue(bytes(SAMPLE));
        const wireType = {id: 'Digest', underlying: {id: 'Float8'}, accuracy: 10000};
        expect(encodeParams([digest])).toEqual([{type: wireType, value: SAMPLE_HEX}]);
        expect(encodeParams({d: Option.some(digest)})).toEqual({d: {type: {id: 'Option', underlying: wireType}, value: SAMPLE_HEX}});
        expect(encodeParams([Option.none(FLOAT8)])).toEqual([{type: {id: 'Option', underlying: wireType}, value: noneMarker(0)}]);
    });

    it('names the digest type when an option of digest is unwrapped as none', () => {
        expect(() => Option.none(FLOAT8).unwrap()).toThrow('unwrap on a None of type Digest(Float8, 0.01)');
    });
});
