// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {DigestInnerType, DigestType, TypeValuePair, Value} from ".";
import {TYPE_CODE} from "../type-code";

const VERSION = 1n;
const MIN_ACCURACY = 1_000;
const MAX_ACCURACY = 100_000;
const U32_MAX = 0xffff_ffffn;
const U64_MAX = 0xffff_ffff_ffff_ffffn;
const I32_MAX = 0x7fff_ffffn;

const INNER_TYPES: readonly DigestInnerType[] = [
    "Float4", "Float8",
    "Int1", "Int2", "Int4", "Int8", "Int16",
    "Uint1", "Uint2", "Uint4", "Uint8", "Uint16",
    "Duration",
];

export function digestType(inner: string, accuracy: number): DigestType {
    const supported = INNER_TYPES.find(name => name === inner);
    if (supported === undefined) {
        throw new Error(`digest does not support ${inner} input`);
    }
    if (!Number.isInteger(accuracy) || accuracy < MIN_ACCURACY || accuracy > MAX_ACCURACY) {
        throw new Error(`digest accuracy must be a whole number of ppm between ${MIN_ACCURACY} and ${MAX_ACCURACY}, got ${accuracy}`);
    }
    return {Digest: {inner: supported, accuracy}};
}

export function digestTypeName(type: DigestType): string {
    const {inner, accuracy} = type.Digest;
    const whole = Math.floor(accuracy / 1_000_000);
    const fraction = accuracy % 1_000_000;
    const text = fraction === 0 ? `${whole}` : `${whole}.${String(fraction).padStart(6, "0").replace(/0+$/, "")}`;
    return `Digest(${inner}, ${text})`;
}

export class DigestValue implements Value {
    readonly type: DigestType;
    readonly inner: DigestInnerType;
    readonly accuracy: number;
    readonly count: bigint;
    private readonly bytes: Uint8Array;

    constructor(bytes: Uint8Array) {
        const reader = new DigestReader(bytes);
        const decoded = reader.digest();
        this.bytes = new Uint8Array(bytes);
        this.type = decoded.type;
        this.inner = decoded.type.Digest.inner;
        this.accuracy = decoded.type.Digest.accuracy;
        this.count = decoded.count;
    }

    static parse(str: string, type: DigestType): DigestValue {
        if (!/^0x([0-9a-fA-F]{2})*$/.test(str)) {
            throw new Error(`Cannot parse "${str}" as ${digestTypeName(type)}`);
        }
        const bytes = new Uint8Array((str.length - 2) / 2);
        for (let i = 0; i < bytes.length; i++) {
            bytes[i] = parseInt(str.substring(2 + i * 2, 4 + i * 2), 16);
        }
        const digest = new DigestValue(bytes);
        if (digest.inner !== type.Digest.inner || digest.accuracy !== type.Digest.accuracy) {
            throw new Error(`digest is ${digestTypeName(digest.type)} but the type is ${digestTypeName(type)}`);
        }
        return digest;
    }

    asBytes(): Uint8Array {
        return new Uint8Array(this.bytes);
    }

    toString(): string {
        return `digest(n: ${this.count})`;
    }

    equals(other: Value): boolean {
        if (!(other instanceof DigestValue) || other.bytes.length !== this.bytes.length) {
            return false;
        }
        return this.bytes.every((byte, i) => byte === other.bytes[i]);
    }

    toJSON(): string {
        return hex(this.bytes);
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: hex(this.bytes)
        };
    }
}

function hex(bytes: Uint8Array): string {
    return "0x" + Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
}

class DigestReader {
    private position = 0;

    constructor(private readonly bytes: Uint8Array) {}

    digest(): {type: DigestType; count: bigint} {
        const version = this.varint();
        if (version !== VERSION) {
            throw new Error(`digest encoding version ${version} is unknown`);
        }
        const tag = this.varint();
        const inner = INNER_TYPES.find(name => BigInt(TYPE_CODE[name]) === tag);
        if (inner === undefined) {
            throw new Error(`digest encoding has unknown inner type tag ${tag}`);
        }
        const accuracy = this.varint();
        if (accuracy > U32_MAX) {
            throw new Error(`digest accuracy must be a whole number of ppm between ${MIN_ACCURACY} and ${MAX_ACCURACY}, got ${accuracy}`);
        }
        const type = digestType(inner, Number(accuracy));
        const zero = this.varint();
        const negativeInfinity = this.varint();
        const positiveInfinity = this.varint();
        const negative = this.store();
        const positive = this.store();
        if (this.position !== this.bytes.length) {
            throw new Error(`digest encoding has ${this.bytes.length - this.position} trailing bytes`);
        }
        if (inner === "Duration" && (negativeInfinity !== 0n || positiveInfinity !== 0n)) {
            throw new Error("digest of duration cannot hold infinity");
        }
        const count = zero + negativeInfinity + positiveInfinity + negative + positive;
        if (count > U64_MAX) {
            throw new Error("digest encoding counts overflow u64");
        }
        return {type, count};
    }

    private varint(): bigint {
        let value = 0n;
        let shift = 0n;
        for (;;) {
            if (this.position >= this.bytes.length) {
                throw new Error("digest encoding ends early");
            }
            const byte = this.bytes[this.position];
            this.position += 1;
            const low = BigInt(byte & 0x7f);
            if (shift === 63n && low > 1n) {
                throw new Error("digest encoding has a varint above u64");
            }
            value |= low << shift;
            if ((byte & 0x80) === 0) {
                if (byte === 0 && shift > 0n) {
                    throw new Error("digest encoding has a varint that is not minimal");
                }
                return value;
            }
            shift += 7n;
            if (shift > 63n) {
                throw new Error("digest encoding has a varint above u64");
            }
        }
    }

    private store(): bigint {
        const len = this.varint();
        let total = 0n;
        let previous: bigint | undefined;
        for (let i = 0n; i < len; i++) {
            let index: bigint;
            if (previous === undefined) {
                const encoded = this.varint();
                if (encoded > U32_MAX) {
                    throw new Error("digest encoding has a bucket index outside i32");
                }
                index = (encoded >> 1n) ^ -(encoded & 1n);
            } else {
                const delta = this.varint();
                if (delta === 0n) {
                    throw new Error(`digest encoding repeats bucket index ${previous}`);
                }
                index = previous + delta;
                if (index > I32_MAX) {
                    throw new Error("digest encoding has a bucket index outside i32");
                }
            }
            const count = this.varint();
            if (count === 0n) {
                throw new Error(`digest encoding has a zero count at bucket ${index}`);
            }
            total += count;
            previous = index;
        }
        return total;
    }
}
