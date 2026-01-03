// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::FlowNodeId,
	key::{EncodableKey, FlowNodeStateKey},
	value::encoded::{EncodedValues, EncodedValuesLayout},
};
use reifydb_store_transaction::MultiVersionBatch;
use tracing::{Span, instrument};

use super::FlowTransaction;

impl FlowTransaction<'_> {
	/// Get state for a specific flow node and key
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		found
	))]
	pub async fn state_get(&mut self, id: FlowNodeId, key: &EncodedKey) -> crate::Result<Option<EncodedValues>> {
		let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		let result = self.get(&encoded_key).await?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	/// Set state for a specific flow node and key
	#[instrument(name = "flow::state::set", level = "trace", skip(self, value), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		value_len = value.as_ref().len()
	))]
	pub async fn state_set(&mut self, id: FlowNodeId, key: &EncodedKey, value: EncodedValues) -> crate::Result<()> {
		let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		self.set(&encoded_key, value).await
	}

	/// Remove state for a specific flow node and key
	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub async fn state_remove(&mut self, id: FlowNodeId, key: &EncodedKey) -> crate::Result<()> {
		let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		self.remove(&encoded_key).await
	}

	/// Scan all state for a specific flow node
	#[instrument(name = "flow::state::scan", level = "debug", skip(self), fields(
		node_id = id.0
	))]
	pub async fn state_scan(&mut self, id: FlowNodeId) -> crate::Result<MultiVersionBatch> {
		let range = FlowNodeStateKey::node_range(id);
		self.range(range).await
	}

	/// Range query on state for a specific flow node
	#[instrument(name = "flow::state::range", level = "debug", skip(self, range), fields(
		node_id = id.0
	))]
	pub async fn state_range(
		&mut self,
		id: FlowNodeId,
		range: EncodedKeyRange,
	) -> crate::Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(FlowNodeStateKey::encoded(id, vec![]));
		self.range(prefixed_range).await
	}

	/// Clear all state for a specific flow node
	#[instrument(name = "flow::state::clear", level = "debug", skip(self), fields(
		node_id = id.0,
		removed_count
	))]
	pub async fn state_clear(&mut self, id: FlowNodeId) -> crate::Result<()> {
		let range = FlowNodeStateKey::node_range(id);
		let batch = self.range(range).await?;
		let keys_to_remove: Vec<_> = batch.items.into_iter().map(|multi| multi.key).collect();

		let count = keys_to_remove.len();
		for key in keys_to_remove {
			self.remove(&key).await?;
		}

		Span::current().record("removed_count", count);
		Ok(())
	}

	/// Load state for a key, creating if not exists
	#[instrument(name = "flow::state::load_or_create", level = "debug", skip(self, layout), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		created
	))]
	pub async fn load_or_create_row(
		&mut self,
		id: FlowNodeId,
		key: &EncodedKey,
		layout: &EncodedValuesLayout,
	) -> crate::Result<EncodedValues> {
		match self.state_get(id, key).await? {
			Some(row) => {
				Span::current().record("created", false);
				Ok(row)
			}
			None => {
				Span::current().record("created", true);
				Ok(layout.allocate())
			}
		}
	}

	/// Save state encoded
	#[instrument(name = "flow::state::save", level = "trace", skip(self, row), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub async fn save_row(&mut self, id: FlowNodeId, key: &EncodedKey, row: EncodedValues) -> crate::Result<()> {
		self.state_set(id, key, row).await
	}
}
