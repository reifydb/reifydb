// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::row_number::RowNumber;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowSettings {
	pub ttl: Option<Ttl>,

	pub persistent: bool,
}

impl RowSettings {
	pub fn is_persistent(&self) -> bool {
		self.persistent
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorSettings {
	pub ttl: Option<Ttl>,

	pub join: Option<JoinTtl>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinTtl {
	pub left: Option<Ttl>,

	pub right: Option<Ttl>,
}
