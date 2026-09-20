// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;

use crate::lsn::Lsn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursors {
	pub appended: Option<Lsn>,
	pub durable: Option<Lsn>,
	pub start: Option<Lsn>,
	pub floors: Vec<(String, Option<Lsn>)>,
	pub bytes: ByteSize,
}
