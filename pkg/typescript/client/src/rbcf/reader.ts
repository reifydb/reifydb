// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Little-endian binary reader over a Uint8Array. Used by the RBCF decoder.

export class BinaryReader {
    private readonly view: DataView;
    public pos: number;

    constructor(public readonly buf: Uint8Array, pos: number = 0) {
        this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
        this.pos = pos;
    }

    remaining(): number {
        return this.buf.length - this.pos;
    }

    ensure(n: number): void {
        if (this.pos + n > this.buf.length) {
            throw new Error(`RBCF: unexpected EOF, need ${n} bytes, have ${this.remaining()}`);
        }
    }

    skip(n: number): void {
        this.ensure(n);
        this.pos += n;
    }

    u8(): number {
        this.ensure(1);
        return this.view.getUint8(this.pos++);
    }

    i8(): number {
        this.ensure(1);
        return this.view.getInt8(this.pos++);
    }

    u16(): number {
        this.ensure(2);
        const v = this.view.getUint16(this.pos, true);
        this.pos += 2;
        return v;
    }

    i16(): number {
        this.ensure(2);
        const v = this.view.getInt16(this.pos, true);
        this.pos += 2;
        return v;
    }

    u32(): number {
        this.ensure(4);
        const v = this.view.getUint32(this.pos, true);
        this.pos += 4;
        return v;
    }

    i32(): number {
        this.ensure(4);
        const v = this.view.getInt32(this.pos, true);
        this.pos += 4;
        return v;
    }

    u64(): bigint {
        this.ensure(8);
        const v = this.view.getBigUint64(this.pos, true);
        this.pos += 8;
        return v;
    }

    i64(): bigint {
        this.ensure(8);
        const v = this.view.getBigInt64(this.pos, true);
        this.pos += 8;
        return v;
    }

    f32(): number {
        this.ensure(4);
        const v = this.view.getFloat32(this.pos, true);
        this.pos += 4;
        return v;
    }

    f64(): number {
        this.ensure(8);
        const v = this.view.getFloat64(this.pos, true);
        this.pos += 8;
        return v;
    }

    // i128 / u128 (16 bytes) — returned as bigint since JS has no native 128-bit int.
    i128(): bigint {
        this.ensure(16);
        const lo = this.view.getBigUint64(this.pos, true);
        const hi = this.view.getBigInt64(this.pos + 8, true);
        this.pos += 16;
        return (hi << 64n) | lo;
    }

    u128(): bigint {
        this.ensure(16);
        const lo = this.view.getBigUint64(this.pos, true);
        const hi = this.view.getBigUint64(this.pos + 8, true);
        this.pos += 16;
        return (hi << 64n) | lo;
    }

    bytes(n: number): Uint8Array {
        this.ensure(n);
        // subarray shares the underlying buffer; consumers should not mutate
        const out = this.buf.subarray(this.pos, this.pos + n);
        this.pos += n;
        return out;
    }

    utf8(n: number): string {
        const slice = this.bytes(n);
        return new TextDecoder("utf-8").decode(slice);
    }
}

// Standalone readers for pre-sliced byte arrays (used inside per-column decoders).
export function read_u8(buf: Uint8Array, pos: number): number {
    return buf[pos];
}
export function read_i8(buf: Uint8Array, pos: number): number {
    const v = buf[pos];
    return v > 0x7f ? v - 0x100 : v;
}
export function read_u16(buf: Uint8Array, pos: number): number {
    return buf[pos] | (buf[pos + 1] << 8);
}
export function read_i16(buf: Uint8Array, pos: number): number {
    const v = read_u16(buf, pos);
    return v > 0x7fff ? v - 0x10000 : v;
}
export function read_u32(buf: Uint8Array, pos: number): number {
    // >>> 0 to keep it as unsigned 32-bit
    return (buf[pos] | (buf[pos + 1] << 8) | (buf[pos + 2] << 16) | (buf[pos + 3] << 24)) >>> 0;
}
export function read_i32(buf: Uint8Array, pos: number): number {
    return (buf[pos] | (buf[pos + 1] << 8) | (buf[pos + 2] << 16) | (buf[pos + 3] << 24)) | 0;
}
export function read_u64(buf: Uint8Array, pos: number): bigint {
    const lo = BigInt(read_u32(buf, pos));
    const hi = BigInt(read_u32(buf, pos + 4));
    return (hi << 32n) | lo;
}
export function read_i64(buf: Uint8Array, pos: number): bigint {
    const u = read_u64(buf, pos);
    // sign-extend via 64-bit check
    return u >= 0x8000000000000000n ? u - 0x10000000000000000n : u;
}
export function read_f32(buf: Uint8Array, pos: number): number {
    const dv = new DataView(buf.buffer, buf.byteOffset + pos, 4);
    return dv.getFloat32(0, true);
}
export function read_f64(buf: Uint8Array, pos: number): number {
    const dv = new DataView(buf.buffer, buf.byteOffset + pos, 8);
    return dv.getFloat64(0, true);
}
export function read_i128(buf: Uint8Array, pos: number): bigint {
    const lo = read_u64(buf, pos);
    const hi = read_i64(buf, pos + 8);
    return (hi << 64n) | lo;
}
export function read_u128(buf: Uint8Array, pos: number): bigint {
    const lo = read_u64(buf, pos);
    const hi = read_u64(buf, pos + 8);
    return (hi << 64n) | lo;
}
