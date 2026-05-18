// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// RBCF: ReifyDB Binary Columnar Format v1.
// Wire layout mirrors crates/wire-format/src/format.rs exactly.

export const RBCF_MAGIC = 0x46434252; // "RBCF" little-endian
export const RBCF_VERSION = 1;

export const MESSAGE_HEADER_SIZE = 16;
export const FRAME_HEADER_SIZE = 12;
export const COLUMN_DESCRIPTOR_SIZE = 28;

export const META_HAS_ROW_NUMBERS = 1 << 0;
export const META_HAS_CREATED_AT = 1 << 1;
export const META_HAS_UPDATED_AT = 1 << 2;

export const COL_FLAG_HAS_NONES = 1 << 0;

export enum ColumnEncoding {
    Plain = 0,
    Dict = 1,
    Rle = 2,
    Delta = 3,
    BitPack = 4,
    DeltaRle = 5,
}

export function dict_index_width_from_flags(flags: number): number {
    switch ((flags >> 4) & 0x03) {
        case 0: return 1;
        case 1: return 2;
        case 2: return 4;
        default: return 4;
    }
}

export function dict_index_width_to_flags(width: number): number {
    switch (width) {
        case 1: return 0 << 4;
        case 2: return 1 << 4;
        case 4: return 2 << 4;
        default: return 2 << 4;
    }
}

// Type codes — mirrors Type::to_u8 / Type::from_u8 in crates/type/src/value/type/mod.rs.
// High bit 0x80 denotes Option(T).
export const TYPE_OPTION_FLAG = 0x80;

export const TYPE_CODE = {
    Float4: 1,
    Float8: 2,
    Int1: 3,
    Int2: 4,
    Int4: 5,
    Int8: 6,
    Int16: 7,
    Utf8: 8,
    Uint1: 9,
    Uint2: 10,
    Uint4: 11,
    Uint8: 12,
    Uint16: 13,
    Boolean: 14,
    Date: 15,
    DateTime: 16,
    Time: 17,
    Duration: 18,
    IdentityId: 19,
    Uuid4: 20,
    Uuid7: 21,
    Blob: 22,
    Int: 23,
    Decimal: 24,
    Uint: 25,
    Any: 26,
    DictionaryId: 27,
} as const;

export type TypeName = keyof typeof TYPE_CODE;

const CODE_TO_NAME: Record<number, TypeName> = Object.fromEntries(
    Object.entries(TYPE_CODE).map(([k, v]) => [v, k as TypeName])
);

export function type_name_from_code(code: number): TypeName {
    const base = code & 0x7F;
    const name = CODE_TO_NAME[base];
    if (!name) throw new Error(`Unknown RBCF type code: ${code}`);
    return name;
}

export function is_option_code(code: number): boolean {
    return (code & TYPE_OPTION_FLAG) !== 0;
}
