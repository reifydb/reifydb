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

export function dictIndexWidthFromFlags(flags: number): number {
    switch ((flags >> 4) & 0x03) {
        case 0: return 1;
        case 1: return 2;
        case 2: return 4;
        default: return 4;
    }
}

export function dictIndexWidthToFlags(width: number): number {
    switch (width) {
        case 1: return 0 << 4;
        case 2: return 1 << 4;
        case 4: return 2 << 4;
        default: return 2 << 4;
    }
}

export {TYPE_CODE, typeNameFromCode} from '@reifydb/core';
export type {TypeName} from '@reifydb/core';

// TypeTag byte layout for typeinfo bytes: (option_depth << 6) | kind.
export const TAG_KIND_MASK = 0x3f;
export const TAG_DEPTH_SHIFT = 6;
export const RESERVED_KIND = 63;
export const EXTENDED_TYPE_TAG = 0xff;
