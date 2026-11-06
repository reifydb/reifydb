// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod queue;
pub mod ring;
pub mod series;
pub mod table;

use reifydb_codec::row::bytes::EncodedBytes;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Written {
	pub written: Vec<EncodedBytes>,
	pub replaced: Vec<EncodedBytes>,
}
