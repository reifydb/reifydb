// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Little-endian binary writer backed by a growable Uint8Array.

export class BinaryWriter {
    private buf: Uint8Array;
    private view: DataView;
    private _length: number;

    constructor(initialCapacity: number = 4096) {
        this.buf = new Uint8Array(initialCapacity);
        this.view = new DataView(this.buf.buffer);
        this._length = 0;
    }

    get length(): number {
        return this._length;
    }

    private grow(need: number): void {
        if (this._length + need <= this.buf.length) return;
        let cap = this.buf.length;
        while (cap < this._length + need) cap *= 2;
        const next = new Uint8Array(cap);
        next.set(this.buf.subarray(0, this._length));
        this.buf = next;
        this.view = new DataView(this.buf.buffer);
    }

    reserve(n: number): number {
        this.grow(n);
        const start = this._length;
        this._length += n;
        return start;
    }

    u8(v: number): void {
        this.grow(1);
        this.buf[this._length++] = v & 0xff;
    }

    i8(v: number): void {
        this.u8(v & 0xff);
    }

    u16(v: number): void {
        this.grow(2);
        this.view.setUint16(this._length, v, true);
        this._length += 2;
    }

    i16(v: number): void {
        this.grow(2);
        this.view.setInt16(this._length, v, true);
        this._length += 2;
    }

    u32(v: number): void {
        this.grow(4);
        this.view.setUint32(this._length, v >>> 0, true);
        this._length += 4;
    }

    i32(v: number): void {
        this.grow(4);
        this.view.setInt32(this._length, v | 0, true);
        this._length += 4;
    }

    u64(v: bigint): void {
        this.grow(8);
        this.view.setBigUint64(this._length, v, true);
        this._length += 8;
    }

    i64(v: bigint): void {
        this.grow(8);
        this.view.setBigInt64(this._length, v, true);
        this._length += 8;
    }

    f32(v: number): void {
        this.grow(4);
        this.view.setFloat32(this._length, v, true);
        this._length += 4;
    }

    f64(v: number): void {
        this.grow(8);
        this.view.setFloat64(this._length, v, true);
        this._length += 8;
    }

    i128(v: bigint): void {
        const mask = (1n << 64n) - 1n;
        const lo = v & mask;
        const hi = v >> 64n;
        this.u64(lo);
        this.i64(hi);
    }

    u128(v: bigint): void {
        const mask = (1n << 64n) - 1n;
        const lo = v & mask;
        const hi = v >> 64n;
        this.u64(lo);
        this.u64(hi);
    }

    bytes(b: Uint8Array): void {
        this.grow(b.length);
        this.buf.set(b, this._length);
        this._length += b.length;
    }

    utf8(s: string): number {
        const enc = new TextEncoder().encode(s);
        this.bytes(enc);
        return enc.length;
    }

    zeroes(n: number): void {
        this.grow(n);
        this._length += n; // already zero-initialized
    }

    // Overwrite a previously reserved region.
    patchU8(offset: number, v: number): void {
        this.buf[offset] = v & 0xff;
    }
    patchU16(offset: number, v: number): void {
        this.view.setUint16(offset, v, true);
    }
    patchU32(offset: number, v: number): void {
        this.view.setUint32(offset, v >>> 0, true);
    }
    patchBytes(offset: number, b: Uint8Array): void {
        this.buf.set(b, offset);
    }

    finish(): Uint8Array {
        return this.buf.subarray(0, this._length);
    }
}
