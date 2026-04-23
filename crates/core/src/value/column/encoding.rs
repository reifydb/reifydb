// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Identifier for a column encoding. Canonical encodings (identity wraps) and
// compressed encoding ids are listed here so they can be referenced from the
// `ColumnData` trait defaults in reifydb-core. The matching `Encoding` trait
// implementations live in `reifydb-column`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EncodingId(pub &'static str);

impl EncodingId {
	pub const CANONICAL_BOOL: EncodingId = EncodingId("column.canonical.bool");
	pub const CANONICAL_FIXED: EncodingId = EncodingId("column.canonical.fixed");
	pub const CANONICAL_VARLEN: EncodingId = EncodingId("column.canonical.varlen");
	pub const CANONICAL_BIGNUM: EncodingId = EncodingId("column.canonical.bignum");

	pub const CONSTANT: EncodingId = EncodingId("column.constant");
	pub const ALL_NONE: EncodingId = EncodingId("column.all_none");
	pub const DICT: EncodingId = EncodingId("column.dict");
	pub const RLE: EncodingId = EncodingId("column.rle");
	pub const DELTA: EncodingId = EncodingId("column.delta");
	pub const DELTA_RLE: EncodingId = EncodingId("column.delta_rle");
	pub const FOR: EncodingId = EncodingId("column.for");
	pub const BITPACK: EncodingId = EncodingId("column.bitpack");
	pub const SPARSE: EncodingId = EncodingId("column.sparse");
}
