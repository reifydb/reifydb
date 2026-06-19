// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::store::MultiVersionRow,
};
use reifydb_value::Result;

pub mod counter;
pub mod keyed;
pub mod raw;
pub mod row;
pub mod single;
pub mod test_utils;
pub mod utils;
pub mod window;

use reifydb_core::key::{
	EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey,
};

pub struct StateIterator<'a> {
	inner: Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>,
}

impl<'a> StateIterator<'a> {
	pub fn new(inner: Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>) -> Self {
		Self {
			inner,
		}
	}
}

impl Iterator for StateIterator<'_> {
	type Item = Result<(EncodedKey, EncodedRow)>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.inner.next()? {
			Ok(multi) => {
				let pair = if let Some(state_key) = FlowNodeStateKey::decode(&multi.key) {
					(EncodedKey::new(state_key.key), multi.row)
				} else if let Some(internal_key) = FlowNodeInternalStateKey::decode(&multi.key) {
					(EncodedKey::new(internal_key.key), multi.row)
				} else {
					(multi.key, multi.row)
				};
				Some(Ok(pair))
			}
			Err(e) => Some(Err(e)),
		}
	}
}

/// Like [`StateIterator`] but also yields the per-key `CommitVersion`. Used by TTL eviction, which
/// is version-anchored: an entry is expired once its version is at or below the epoch cutoff.
pub struct StateIteratorVersioned<'a> {
	inner: Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>,
}

impl<'a> StateIteratorVersioned<'a> {
	pub fn new(inner: Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>) -> Self {
		Self {
			inner,
		}
	}
}

impl Iterator for StateIteratorVersioned<'_> {
	type Item = Result<(EncodedKey, CommitVersion, EncodedRow)>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.inner.next()? {
			Ok(multi) => {
				let version = multi.version;
				let key = if let Some(state_key) = FlowNodeStateKey::decode(&multi.key) {
					EncodedKey::new(state_key.key)
				} else if let Some(internal_key) = FlowNodeInternalStateKey::decode(&multi.key) {
					EncodedKey::new(internal_key.key)
				} else {
					multi.key
				};
				Some(Ok((key, version, multi.row)))
			}
			Err(e) => Some(Err(e)),
		}
	}
}
