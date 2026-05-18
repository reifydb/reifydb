// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Nones bitmap: LSB-first per byte. A set bit means the value is defined (present).
// Mirrors Rust BitVec::from_raw used in crates/wire-format/src/encoding/plain.rs:encode_bitvec
// and reifydb_type::util::bitvec::BitVec semantics.

export function decode_bitvec(data: Uint8Array, len: number): boolean[] {
    const out = new Array<boolean>(len);
    for (let i = 0; i < len; i++) {
        const byte = data[i >> 3] ?? 0;
        out[i] = ((byte >> (i & 7)) & 1) === 1;
    }
    return out;
}

export function encode_bitvec(bits: boolean[]): Uint8Array {
    const byte_count = (bits.length + 7) >> 3;
    const out = new Uint8Array(byte_count);
    for (let i = 0; i < bits.length; i++) {
        if (bits[i]) out[i >> 3] |= 1 << (i & 7);
    }
    return out;
}
