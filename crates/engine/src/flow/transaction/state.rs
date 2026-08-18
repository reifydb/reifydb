// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	row::{operator::EncodedOperatorRow, shape::RowShape},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::Result;
use tracing::{Span, field, instrument};

use super::FlowTransaction;

#[derive(Clone, Copy)]
enum StateScope {
	Public,
	Internal,
}

impl StateScope {
	fn encode(self, id: OperatorId, key: &EncodedKey) -> EncodedKey {
		match self {
			StateScope::Public => OperatorStateKey::encoded(id, GroupId::ROOT, Keyspace::CUSTOM, key.as_ref()),
			StateScope::Internal => {
				OperatorStateKey::encoded(id, GroupId::ROOT, Keyspace::ENGINE_META, key.as_ref())
			}
		}
	}
}

impl FlowTransaction<'_, '_> {
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		found = field::Empty
	))]
	pub fn state_get(&mut self, id: OperatorId, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
		let result = self.scoped_get(StateScope::Public, id, key)?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::state::get_many", level = "debug", skip(self, keys), fields(
		node_id = id.0,
		key_count = keys.len(),
		found_count = field::Empty
	))]
	pub fn state_get_many(&mut self, id: OperatorId, keys: &[EncodedKey]) -> Result<MultiVersionBatch> {
		let batch = self.scoped_get_many(StateScope::Public, id, keys)?;
		Span::current().record("found_count", batch.items.len());
		Ok(batch)
	}

	#[instrument(name = "flow::state::set", level = "trace", skip(self, value), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		value_len = value.len()
	))]
	pub fn state_set(&mut self, id: OperatorId, key: &EncodedKey, value: EncodedOperatorRow) -> Result<()> {
		self.scoped_set(StateScope::Public, id, key, value)
	}

	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn state_remove(&mut self, id: OperatorId, key: &EncodedKey) -> Result<()> {
		self.scoped_remove(StateScope::Public, id, key)
	}

	#[instrument(name = "flow::state::drop", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn state_drop(&mut self, id: OperatorId, key: &EncodedKey) -> Result<()> {
		self.scoped_drop(StateScope::Public, id, key)
	}

	#[instrument(name = "flow::internal_state::get", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		found = field::Empty
	))]
	pub fn internal_state_get(&mut self, id: OperatorId, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
		let result = self.scoped_get(StateScope::Internal, id, key)?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::internal_state::get_many", level = "debug", skip(self, keys), fields(
		node_id = id.0,
		key_count = keys.len(),
		found_count = field::Empty
	))]
	pub fn internal_state_get_many(&mut self, id: OperatorId, keys: &[EncodedKey]) -> Result<MultiVersionBatch> {
		let batch = self.scoped_get_many(StateScope::Internal, id, keys)?;
		Span::current().record("found_count", batch.items.len());
		Ok(batch)
	}

	#[instrument(name = "flow::internal_state::set", level = "trace", skip(self, value), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		value_len = value.len()
	))]
	pub fn internal_state_set(&mut self, id: OperatorId, key: &EncodedKey, value: EncodedOperatorRow) -> Result<()> {
		self.scoped_set(StateScope::Internal, id, key, value)
	}

	#[instrument(name = "flow::internal_state::remove", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn internal_state_remove(&mut self, id: OperatorId, key: &EncodedKey) -> Result<()> {
		self.scoped_remove(StateScope::Internal, id, key)
	}

	#[instrument(name = "flow::internal_state::drop", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn internal_state_drop(&mut self, id: OperatorId, key: &EncodedKey) -> Result<()> {
		self.scoped_drop(StateScope::Internal, id, key)
	}

	#[instrument(name = "flow::state::scan", level = "debug", skip(self), fields(
		node_id = id.0,
		result_count = field::Empty
	))]
	pub fn state_scan_all(&mut self, id: OperatorId) -> Result<MultiVersionBatch> {
		let range = OperatorStateKey::node_range(id);
		let iter = self.range(range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			items.push(result?);
		}
		Span::current().record("result_count", items.len());
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::state::range", level = "debug", skip(self, range), fields(
		node_id = id.0
	))]
	pub fn state_range_all(&mut self, id: OperatorId, range: EncodedKeyRange) -> Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, GroupId::ROOT, Keyspace::CUSTOM, []));
		let iter = self.range(prefixed_range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			items.push(result?);
		}
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::internal_state::range", level = "debug", skip(self, range), fields(
		node_id = id.0
	))]
	pub fn internal_state_range(
		&mut self,
		id: OperatorId,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, GroupId::ROOT, Keyspace::ENGINE_META, []));
		let iter = self.range(prefixed_range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			if limit.is_some_and(|l| items.len() == l) {
				return Ok(MultiVersionBatch {
					items,
					has_more: true,
				});
			}
			items.push(result?);
		}
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::state::clear", level = "trace", skip(self), fields(
		node_id = id.0,
		keys_removed = field::Empty
	))]
	pub fn state_clear(&mut self, id: OperatorId) -> Result<()> {
		let keys_to_remove = self.scan_keys_for_clear(id)?;

		let count = keys_to_remove.len();
		self.remove_keys(keys_to_remove)?;

		Span::current().record("keys_removed", count);
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::state::clear::scan", level = "trace", skip(self), fields(node_id = id.0))]
	fn scan_keys_for_clear(&mut self, id: OperatorId) -> Result<Vec<EncodedKey>> {
		let range = OperatorStateKey::node_range(id);
		let iter = self.range(range, RangeScope::All, 1024);
		let mut keys = Vec::new();
		for result in iter {
			let multi = result?;
			keys.push(multi.key);
		}
		Ok(keys)
	}

	#[inline]
	#[instrument(name = "flow::state::clear::remove", level = "trace", skip(self, keys), fields(count = keys.len()))]
	fn remove_keys(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
		for key in keys {
			self.remove(&key)?;
		}
		Ok(())
	}

	#[instrument(name = "flow::state::load_or_create", level = "debug", skip(self, shape), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		created
	))]
	pub fn load_or_create_row(&mut self, id: OperatorId, key: &EncodedKey, shape: &RowShape) -> Result<EncodedOperatorRow> {
		match self.state_get(id, key)? {
			Some(row) => {
				Span::current().record("created", false);
				Ok(row)
			}
			None => {
				Span::current().record("created", true);
				Ok(shape.allocate_operator().freeze())
			}
		}
	}

	#[instrument(name = "flow::state::save", level = "trace", skip(self, row), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn save_row(&mut self, id: OperatorId, key: &EncodedKey, row: EncodedOperatorRow) -> Result<()> {
		self.state_set(id, key, row)
	}

	fn scoped_get(&mut self, scope: StateScope, id: OperatorId, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
		let encoded_key = scope.encode(id, key);
		self.get(&encoded_key)
	}

	fn scoped_get_many(
		&mut self,
		scope: StateScope,
		id: OperatorId,
		keys: &[EncodedKey],
	) -> Result<MultiVersionBatch> {
		let version = self.version();
		let mut items: Vec<MultiVersionRow> = Vec::new();

		for key in keys {
			let encoded_key = scope.encode(id, key);
			if let Some(row) = self.get(&encoded_key)? {
				items.push(MultiVersionRow {
					key: encoded_key,
					bytes: row.into_bytes(),
					version,
				});
			}
		}

		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	fn scoped_set(&mut self, scope: StateScope, id: OperatorId, key: &EncodedKey, value: EncodedOperatorRow) -> Result<()> {
		self.set(&scope.encode(id, key), value)
	}

	fn scoped_remove(&mut self, scope: StateScope, id: OperatorId, key: &EncodedKey) -> Result<()> {
		let encoded_key = scope.encode(id, key);
		self.remove(&encoded_key)
	}

	fn scoped_drop(&mut self, scope: StateScope, id: OperatorId, key: &EncodedKey) -> Result<()> {
		let encoded_key = scope.encode(id, key);
		self.remove_silent(&encoded_key)
	}
}
