// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Bound::{Excluded, Included, Unbounded};

use reifydb_core::{EncodedKey, EncodedKeyRange, interface::Key, key::KeyKind, value::encoded::EncodedValues};
use reifydb_store_transaction::MultiVersionBatch;

use super::FlowTransaction;

impl FlowTransaction<'_> {
	/// Get a value by key, routing to cmd for flow state or primitive_query for source data
	pub async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<EncodedValues>> {
		if Self::is_flow_state_key(key) {
			// Flow state: read from cmd (latest version)
			match self.cmd.get(key).await? {
				Some(multi) => Ok(Some(multi.values.clone())),
				None => Ok(None),
			}
		} else {
			// Source data: read from primitive_query (CDC version)
			match self.primitive_query.get(key).await? {
				Some(multi) => Ok(Some(multi.values().clone())),
				None => Ok(None),
			}
		}
	}

	/// Check if a key exists
	pub async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		if Self::is_flow_state_key(key) {
			self.cmd.contains_key(key).await
		} else {
			self.primitive_query.contains_key(key).await
		}
	}

	/// Range query
	pub async fn range(&mut self, range: EncodedKeyRange) -> crate::Result<MultiVersionBatch> {
		let is_state_range = match range.start.as_ref() {
			Included(start) | Excluded(start) => Self::is_flow_state_key(start),
			Unbounded => false,
		};

		if is_state_range {
			self.cmd.range(range).await
		} else {
			self.primitive_query.range(range).await
		}
	}

	/// Range query with batching
	pub async fn range_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		let is_state_range = match range.start.as_ref() {
			Included(start) | Excluded(start) => Self::is_flow_state_key(start),
			Unbounded => false,
		};

		if is_state_range {
			self.cmd.range_batch(range, batch_size).await
		} else {
			self.primitive_query.range_batch(range, batch_size).await
		}
	}

	/// Prefix scan
	pub async fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		if Self::is_flow_state_key(prefix) {
			self.cmd.prefix(prefix).await
		} else {
			self.primitive_query.prefix(prefix).await
		}
	}

	fn is_flow_state_key(key: &EncodedKey) -> bool {
		match Key::kind(key) {
			None => false,
			Some(kind) => matches!(kind, KeyKind::FlowNodeState | KeyKind::FlowNodeInternalState),
		}
	}
}
