// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	delta::SetPre,
	interface::{catalog::object::ObjectId, change::Change},
	key::any::TaggedKey,
};
use reifydb_value::Result;

use crate::{change::RowChange, multi::transaction::write::Rebase};

pub trait Write {
	fn set_rebased(&mut self, key: &TaggedKey, bytes: EncodedBytes, rebase: Rebase) -> Result<()>;
	fn set(&mut self, entries: Vec<(TaggedKey, EncodedBytes)>) -> Result<()>;
	fn set_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<()>;
	fn remove_with_pre(&mut self, key: &TaggedKey, pre: EncodedBytes) -> Result<()>;
	fn remove(&mut self, entries: Vec<(TaggedKey, Option<EncodedBytes>)>) -> Result<()>;

	fn track_row_change(&mut self, changes: &[RowChange]);

	fn track_flow_change(&mut self, change: Change);

	fn wants_flow_diff(&self, object: ObjectId) -> bool;

	fn touch_flow_object(&mut self, object: ObjectId);
}
