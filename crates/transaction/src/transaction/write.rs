// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::change::Change,
};
use reifydb_value::Result;

use crate::change::RowChange;

pub trait Write {
	fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()>;
	fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()>;
	fn remove(&mut self, key: &EncodedKey) -> Result<()>;
	fn mark_preexisting(&mut self, key: &EncodedKey) -> Result<()>;

	fn track_row_change(&mut self, changes: &[RowChange]);

	fn track_flow_change(&mut self, change: Change);
}
