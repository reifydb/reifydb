// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::row_number::RowNumber;
use serde::{Deserialize, Serialize};

use crate::encoded::{row::EncodedRow, shape::RowShape};

#[derive(Debug, Clone)]
pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedRow,
	pub shape: RowShape,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ttl {
	pub duration_nanos: u64,

	pub anchor: TtlAnchor,

	pub cleanup_mode: TtlCleanupMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TtlAnchor {
	#[default]
	Created,

	Updated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TtlCleanupMode {
	Delete,

	Drop,
}
