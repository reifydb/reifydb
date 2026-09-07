// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{interface::change::Change, key::any::TaggedKey};
use reifydb_value::Result;

use crate::change::RowChange;

pub trait Write {
	fn set(&mut self, key: &TaggedKey, bytes: EncodedBytes) -> Result<()>;
	fn remove_with_pre(&mut self, key: &TaggedKey, pre: EncodedBytes) -> Result<()>;
	fn remove(&mut self, key: &TaggedKey) -> Result<()>;
	fn mark_preexisting(&mut self, key: &TaggedKey) -> Result<()>;

	fn track_row_change(&mut self, changes: &[RowChange]);

	fn track_flow_change(&mut self, change: Change);
}
