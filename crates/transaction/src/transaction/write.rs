// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::change::Change,
};
use reifydb_type::Result;

use crate::change::RowChange;

pub trait Write {
	fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()>;
	fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()>;
	fn remove(&mut self, key: &EncodedKey) -> Result<()>;
	fn mark_preexisting(&mut self, key: &EncodedKey) -> Result<()>;

	fn track_row_change(&mut self, change: RowChange);

	fn track_flow_change(&mut self, change: Change);
}
