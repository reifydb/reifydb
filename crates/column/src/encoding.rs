// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EncodingId(pub &'static str);

impl EncodingId {
	pub const CANONICAL_BOOL: EncodingId = EncodingId("column.canonical.bool");
	pub const CANONICAL_FIXED: EncodingId = EncodingId("column.canonical.fixed");
	pub const CANONICAL_VARLEN: EncodingId = EncodingId("column.canonical.varlen");
	pub const CANONICAL_BIGNUM: EncodingId = EncodingId("column.canonical.bignum");
}
