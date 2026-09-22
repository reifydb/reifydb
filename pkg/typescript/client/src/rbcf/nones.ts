// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export function decodeBitvec(data: Uint8Array, len: number): boolean[] {
    const out = new Array<boolean>(len);
    for (let i = 0; i < len; i++) {
        const byte = data[i >> 3] ?? 0;
        out[i] = ((byte >> (i & 7)) & 1) === 1;
    }
    return out;
}

export function encodeBitvec(bits: boolean[]): Uint8Array {
    const byteCount = (bits.length + 7) >> 3;
    const out = new Uint8Array(byteCount);
    for (let i = 0; i < bits.length; i++) {
        if (bits[i]) out[i >> 3] |= 1 << (i & 7);
    }
    return out;
}
