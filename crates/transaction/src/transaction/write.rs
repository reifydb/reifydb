// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{interface::change::Change, key::any::AnyKey};
use reifydb_value::Result;

use crate::change::RowChange;

pub trait Write {
	fn set(&mut self, key: &AnyKey, bytes: EncodedBytes) -> Result<()>;
	fn remove_with_pre(&mut self, key: &AnyKey, pre: EncodedBytes) -> Result<()>;
	fn remove(&mut self, key: &AnyKey) -> Result<()>;
	fn mark_preexisting(&mut self, key: &AnyKey) -> Result<()>;

	fn track_row_change(&mut self, changes: &[RowChange]);

	fn track_flow_change(&mut self, change: Change);
}
