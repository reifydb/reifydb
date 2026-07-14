// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// RBCF: ReifyDB Binary Columnar Format.
// Wire layout mirrors crates/codec/src/frame/format.rs exactly.

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

// Value kinds — mirrors ValueKind in crates/codec/src/tag.rs.
// The column descriptor type_code byte is a pure ValueKind byte; column
// optionality is signaled only by COL_FLAG_HAS_NONES plus the none-bitmap.
export const TYPE_CODE = {
    None: 0,
    Boolean: 1,
    Float4: 2,
    Float8: 3,
    Int1: 4,
    Int2: 5,
    Int4: 6,
    Int8: 7,
    Int16: 8,
    Utf8: 9,
    Uint1: 10,
    Uint2: 11,
    Uint4: 12,
    Uint8: 13,
    Uint16: 14,
    Date: 15,
    DateTime: 16,
    Time: 17,
    Duration: 18,
    IdentityId: 19,
    Uuid4: 20,
    Uuid7: 21,
    Blob: 22,
    Int: 23,
    Uint: 24,
    Decimal: 25,
    Any: 26,
    DictionaryId: 27,
    Type: 28,
    List: 29,
    Record: 30,
    Tuple: 31,
    Vector: 32,
} as const;

export type TypeName = keyof typeof TYPE_CODE;

// TypeTag byte layout for typeinfo bytes: (option_depth << 6) | kind.
export const TAG_KIND_MASK = 0x3f;
export const TAG_DEPTH_SHIFT = 6;
export const RESERVED_KIND = 63;
export const EXTENDED_TYPE_TAG = 0xff;

const CODE_TO_NAME: Record<number, TypeName> = Object.fromEntries(
    Object.entries(TYPE_CODE).map(([k, v]) => [v, k as TypeName])
);

export function type_name_from_code(code: number): TypeName {
    const name = CODE_TO_NAME[code];
    if (!name) throw new Error(`Unknown RBCF type code: ${code}`);
    return name;
}

// Decodes a TypeTag byte -- (option_depth << 6) | kind, as produced by type_tag_byte() in
// crates/codec/src/tag.rs and stored in system::columns.type. This is NOT the same as a column
// descriptor type_code, which is a bare kind byte; pass those to type_name_from_code instead.
// Each option level renders as a trailing '?', so Option<Int4> is "Int4?".
export function type_name_from_tag(tag: number): string {
    const kind = tag & TAG_KIND_MASK;
    const depth = tag >> TAG_DEPTH_SHIFT;
    if (kind === RESERVED_KIND) throw new Error(`RBCF: reserved type tag 0x${tag.toString(16)}`);
    return type_name_from_code(kind) + "?".repeat(depth);
}
