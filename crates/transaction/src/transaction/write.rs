// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_core::interface::change::Change;
use reifydb_value::Result;

use crate::change::RowChange;

pub trait Write {
	fn set(&mut self, key: &EncodedKey, bytes: EncodedBytes) -> Result<()>;
	fn remove_with_pre(&mut self, key: &EncodedKey, pre: EncodedBytes) -> Result<()>;
	fn remove(&mut self, key: &EncodedKey) -> Result<()>;
	fn mark_preexisting(&mut self, key: &EncodedKey) -> Result<()>;

	fn track_row_change(&mut self, changes: &[RowChange]);

	fn track_flow_change(&mut self, change: Change);
}
