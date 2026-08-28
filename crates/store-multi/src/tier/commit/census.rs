// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitCensus {
	pub counted: ByteSize,
	pub walked: ByteSize,
}
