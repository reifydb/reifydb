// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
