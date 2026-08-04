// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::{operator_group_state::GroupStateKey, operator_state::OperatorStateKey},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::Result;
use tracing::{Span, field, instrument};

use super::FlowTransaction;

impl FlowTransaction {
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len(),
		found = field::Empty
	))]
	pub fn state_get(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedRow>> {
		let result = self.scoped_get(id, key)?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::state::get_many", level = "debug", skip(self, keys), fields(
		operator_id = id.0,
		key_count = keys.len(),
		found_count = field::Empty
	))]
	pub fn state_get_many(&mut self, id: OperatorId, keys: &[GroupStateKey]) -> Result<MultiVersionBatch> {
		let batch = self.scoped_get_many(id, keys)?;
		Span::current().record("found_count", batch.items.len());
		Ok(batch)
	}

	#[instrument(name = "flow::state::set", level = "trace", skip(self, value), fields(
		operator_id = id.0,
		key_len = key.as_slice().len(),
		value_len = value.len()
	))]
	pub fn state_set(&mut self, id: OperatorId, key: &GroupStateKey, value: EncodedRow) -> Result<()> {
		self.scoped_set(id, key, value)
	}

	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len()
	))]
	pub fn state_remove(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<()> {
		self.scoped_remove(id, key)
	}

	#[instrument(name = "flow::state::scan", level = "debug", skip(self), fields(
		operator_id = id.0,
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
		operator_id = id.0
	))]
	pub fn state_range_all(&mut self, id: OperatorId, range: EncodedKeyRange) -> Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, vec![]));
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

	#[instrument(name = "flow::state::range_limited", level = "debug", skip(self, range), fields(
		operator_id = id.0
	))]
	pub fn state_range(
		&mut self,
		id: OperatorId,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, vec![]));
		let batch_size = limit.map_or(1024, |l| l.saturating_add(1).min(1024));
		let iter = self.range(prefixed_range, RangeScope::All, batch_size);
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
		operator_id = id.0,
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
	#[instrument(name = "flow::state::clear::scan", level = "trace", skip(self), fields(operator_id = id.0))]
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
		operator_id = id.0,
		key_len = key.as_slice().len(),
		created
	))]
	pub fn load_or_create_row(
		&mut self,
		id: OperatorId,
		key: &GroupStateKey,
		shape: &RowShape,
	) -> Result<EncodedRow> {
		match self.state_get(id, key)? {
			Some(row) => {
				Span::current().record("created", false);
				Ok(row)
			}
			None => {
				Span::current().record("created", true);
				Ok(shape.allocate().freeze())
			}
		}
	}

	#[instrument(name = "flow::state::save", level = "trace", skip(self, row), fields(
		operator_id = id.0,
		key_len = key.as_slice().len()
	))]
	pub fn save_row(&mut self, id: OperatorId, key: &GroupStateKey, row: EncodedRow) -> Result<()> {
		self.state_set(id, key, row)
	}

	fn scoped_get(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedRow>> {
		let encoded_key = OperatorStateKey::encoded(id, key.as_slice());
		self.get(&encoded_key)
	}

	fn scoped_get_many(&mut self, id: OperatorId, keys: &[GroupStateKey]) -> Result<MultiVersionBatch> {
		let version = self.version();
		let encoded: Vec<EncodedKey> =
			keys.iter().map(|key| OperatorStateKey::encoded(id, key.as_slice())).collect();

		let mut items: Vec<MultiVersionRow> = Vec::new();
		let mut to_batch: Vec<EncodedKey> = Vec::new();

		for encoded_key in &encoded {
			match self.lookup_overlays(encoded_key) {
				Some(None) => continue,
				Some(Some(row)) => items.push(MultiVersionRow {
					key: encoded_key.clone(),
					row,
					version,
				}),
				None => to_batch.push(encoded_key.clone()),
			}
		}

		self.fetch_external(&to_batch, &mut items)?;

		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[inline]
	fn lookup_overlays(&self, encoded_key: &EncodedKey) -> Option<Option<EncodedRow>> {
		let inner = self.inner();
		let pending = if inner.pending.is_removed(encoded_key) {
			Some(None)
		} else {
			inner.pending.get(encoded_key).map(|row| Some(row.clone()))
		};
		if pending.is_some() {
			return pending;
		}

		if inner.base_pending.is_removed(encoded_key) {
			return Some(None);
		}
		if let Some(row) = inner.base_pending.get(encoded_key) {
			return Some(Some(row.clone()));
		}

		inner.prefetch.get(encoded_key).cloned()
	}

	#[inline]
	fn fetch_external(&mut self, to_batch: &[EncodedKey], items: &mut Vec<MultiVersionRow>) -> Result<()> {
		if to_batch.is_empty() {
			return Ok(());
		}

		if let Self::Ephemeral {
			inner,
			state,
		} = self
		{
			let version = inner.version;
			for encoded_key in to_batch {
				if let Some(row) = state.get(encoded_key) {
					items.push(MultiVersionRow {
						key: encoded_key.clone(),
						row: row.clone(),
						version,
					});
				}
			}
		} else {
			let inner = self.inner_mut();
			inner.store_reads += to_batch.len() as u64;
			let found = inner.state_query.as_ref().unwrap().get_many(to_batch)?;
			for encoded_key in to_batch {
				match found.get(encoded_key) {
					Some(multi) => {
						inner.memoize_prefetch(encoded_key, Some(multi.row.clone()));
						items.push(multi.clone());
					}
					None => {
						inner.memoize_prefetch(encoded_key, None);
					}
				}
			}
		}

		Ok(())
	}

	fn scoped_set(&mut self, id: OperatorId, key: &GroupStateKey, value: EncodedRow) -> Result<()> {
		self.set(&OperatorStateKey::encoded(id, key.as_slice()), value)
	}

	fn scoped_remove(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<()> {
		let encoded_key = OperatorStateKey::encoded(id, key.as_slice());
		self.remove_silent(&encoded_key)
	}
}

#[cfg(test)]
pub mod tests {
	use std::{
		collections::{Bound, HashMap},
		sync::Arc,
	};

	use reifydb_catalog::catalog::Catalog;
	use reifydb_codec::{
		encoded::{
			row::{EncodedRow, SHAPE_HEADER_SIZE},
			shape::RowShape,
		},
		key::encoded::EncodedKeyRange,
	};
	use reifydb_core::{
		actors::pending::{Pending, PendingLayers},
		common::CommitVersion,
		interface::catalog::{flow::OperatorId, id::TableId, storage::StorageId},
		key::{
			EncodableKey,
			operator_group_state::{GroupId, Keyspace, OperatorGroupStateKey},
			row::RowKey,
		},
		state::budget::OperatorStateBudgetHandle,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{datetime::DateTime, identity::IdentityId, row_number::RowNumber, value_type::ValueType},
	};

	use super::*;
	use crate::{
		test_util::create_test_transaction,
		transaction::{
			CommittingParams, DeferredParams, TransactionalParams, read::PREFETCH_MEMO_BYTE_CAP,
			substrate::FlowSubstrate,
		},
	};

	fn commit_state_row(
		engine: &TestEngine,
		operator: OperatorId,
		key: &GroupStateKey,
		row: EncodedRow,
	) -> CommitVersion {
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		cmd.set(&OperatorStateKey::encoded(operator, key.as_slice()), row).unwrap();
		cmd.commit_unchecked().unwrap()
	}

	fn make_key(s: &str) -> GroupStateKey {
		// Framed as an operator composes its keys. A bare byte string encodes as some other group's
		// prefix, so these tests would assert against keys reclamation could prefix-delete.
		OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::FIRST_CUSTOM, s.as_bytes())
	}

	fn make_value(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	fn anchored_row(payload: &[u8], created_at: u64, updated_at: u64) -> EncodedRow {
		let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
		buf[8..16].copy_from_slice(&created_at.to_le_bytes());
		buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
		buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
		EncodedRow(CowVec::new(buf))
	}

	#[test]
	fn test_state_get_set() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);
		let key = make_key("state_key");
		let value = make_value("state_value");

		txn.state_set(operator_id, &key, value.clone()).unwrap();

		let result = txn.state_get(operator_id, &key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_state_get_many() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);
		txn.state_set(operator_id, &make_key("a"), make_value("1")).unwrap();
		txn.state_set(operator_id, &make_key("b"), make_value("2")).unwrap();

		// One namespace, so re-writing a key resolves to the latest value; re-splitting the
		// envelopes would return two rows for "a" here.
		txn.state_set(operator_id, &make_key("a"), make_value("data")).unwrap();

		let batch =
			txn.state_get_many(operator_id, &[make_key("a"), make_key("b"), make_key("missing")]).unwrap();

		// A key with no value is omitted rather than returned empty.
		assert_eq!(batch.items.len(), 2);
		let mut decoded: Vec<(Vec<u8>, EncodedRow)> = batch
			.items
			.iter()
			.map(|item| (OperatorStateKey::decode(&item.key).unwrap().key, item.row.clone()))
			.collect();
		decoded.sort_by(|a, b| a.0.cmp(&b.0));
		assert_eq!(decoded[0], (make_key("a").as_slice().to_vec(), make_value("data")));
		assert_eq!(decoded[1], (make_key("b").as_slice().to_vec(), make_value("2")));
	}

	#[test]
	fn test_state_get_nonexistent() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);
		let key = make_key("missing");

		let result = txn.state_get(operator_id, &key).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_state_remove() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);
		let key = make_key("state_key");
		let value = make_value("state_value");

		txn.state_set(operator_id, &key, value.clone()).unwrap();
		assert_eq!(txn.state_get(operator_id, &key).unwrap(), Some(value));

		txn.state_remove(operator_id, &key).unwrap();
		assert_eq!(txn.state_get(operator_id, &key).unwrap(), None);
	}

	#[test]
	fn test_state_isolation_between_nodes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node1 = OperatorId(1);
		let node2 = OperatorId(2);
		let key = make_key("same_key");

		txn.state_set(node1, &key, make_value("node1_value")).unwrap();
		txn.state_set(node2, &key, make_value("node2_value")).unwrap();

		assert_eq!(txn.state_get(node1, &key).unwrap(), Some(make_value("node1_value")));
		assert_eq!(txn.state_get(node2, &key).unwrap(), Some(make_value("node2_value")));
	}

	#[test]
	fn test_state_scan_all() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);

		txn.state_set(operator_id, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(operator_id, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(operator_id, &make_key("key3"), make_value("value3")).unwrap();

		let iter = txn.state_scan_all(operator_id).unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		assert_eq!(items.len(), 3);
	}

	#[test]
	fn test_state_scan_only_own_node() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node1 = OperatorId(1);
		let node2 = OperatorId(2);

		txn.state_set(node1, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node1, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node2, &make_key("key3"), make_value("value3")).unwrap();

		let items: Vec<_> = txn.state_scan_all(node1).unwrap().items.into_iter().collect();
		assert_eq!(items.len(), 2);

		let items: Vec<_> = txn.state_scan_all(node2).unwrap().items.into_iter().collect();
		assert_eq!(items.len(), 1);
	}

	#[test]
	fn test_state_scan_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);

		let iter = txn.state_scan_all(operator_id).unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[test]
	fn test_state_range_all() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);

		txn.state_set(operator_id, &make_key("a"), make_value("1")).unwrap();
		txn.state_set(operator_id, &make_key("b"), make_value("2")).unwrap();
		txn.state_set(operator_id, &make_key("c"), make_value("3")).unwrap();
		txn.state_set(operator_id, &make_key("d"), make_value("4")).unwrap();

		let range = EncodedKeyRange::new(
			Bound::Included(make_key("b").into_encoded()),
			Bound::Excluded(make_key("d").into_encoded()),
		);
		let iter = txn.state_range_all(operator_id, range).unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		assert_eq!(items.len(), 2);
	}

	#[test]
	fn test_state_clear() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);

		txn.state_set(operator_id, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(operator_id, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(operator_id, &make_key("key3"), make_value("value3")).unwrap();

		assert_eq!(txn.state_scan_all(operator_id).unwrap().items.into_iter().count(), 3);

		txn.state_clear(operator_id).unwrap();

		assert_eq!(txn.state_scan_all(operator_id).unwrap().items.into_iter().count(), 0);
	}

	#[test]
	fn test_state_clear_only_own_node() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node1 = OperatorId(1);
		let node2 = OperatorId(2);

		txn.state_set(node1, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node1, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node2, &make_key("key3"), make_value("value3")).unwrap();

		txn.state_clear(node1).unwrap();

		assert_eq!(txn.state_scan_all(node1).unwrap().items.into_iter().count(), 0);
		assert_eq!(txn.state_scan_all(node2).unwrap().items.into_iter().count(), 1);
	}

	#[test]
	fn test_state_clear_empty_node() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);

		txn.state_clear(operator_id).unwrap();
	}

	#[test]
	fn test_load_or_create_existing() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);
		let key = make_key("key1");
		let value = make_value("existing");
		let shape = RowShape::testing(&[ValueType::Int8, ValueType::Float8]);

		txn.state_set(operator_id, &key, value.clone()).unwrap();

		let result = txn.load_or_create_row(operator_id, &key, &shape).unwrap();
		assert_eq!(result, value);
	}

	#[test]
	fn test_load_or_create_new() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);
		let key = make_key("key1");
		let shape = RowShape::testing(&[ValueType::Int8, ValueType::Float8]);

		let result = txn.load_or_create_row(operator_id, &key, &shape).unwrap();

		assert!(!result.is_empty());
	}

	#[test]
	fn test_save_row() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let operator_id = OperatorId(1);
		let key = make_key("key1");
		let row = make_value("row_data");

		txn.save_row(operator_id, &key, row.clone()).unwrap();

		let result = txn.state_get(operator_id, &key).unwrap();
		assert_eq!(result, Some(row));
	}

	#[test]
	fn test_state_multiple_nodes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node1 = OperatorId(1);
		let node2 = OperatorId(2);
		let node3 = OperatorId(3);

		txn.state_set(node1, &make_key("a"), make_value("n1_a")).unwrap();
		txn.state_set(node1, &make_key("b"), make_value("n1_b")).unwrap();
		txn.state_set(node2, &make_key("a"), make_value("n2_a")).unwrap();
		txn.state_set(node3, &make_key("c"), make_value("n3_c")).unwrap();

		assert_eq!(txn.state_get(node1, &make_key("a")).unwrap(), Some(make_value("n1_a")));
		assert_eq!(txn.state_get(node1, &make_key("b")).unwrap(), Some(make_value("n1_b")));
		assert_eq!(txn.state_get(node2, &make_key("a")).unwrap(), Some(make_value("n2_a")));
		assert_eq!(txn.state_get(node3, &make_key("c")).unwrap(), Some(make_value("n3_c")));

		assert_eq!(txn.state_get(node2, &make_key("b")).unwrap(), None);
		assert_eq!(txn.state_get(node3, &make_key("a")).unwrap(), None);
	}

	#[test]
	fn store_reads_counts_store_reaching_reads_only() {
		// store_reads drives per-operator attribution of store traffic, so it must count exactly
		// the reads that leave the transaction and never the ones the pending overlay serves, or
		// the profiler misattributes the read amplification it exists to measure.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let committed_key = make_key("committed");
		commit_state_row(&engine, operator_id, &committed_key, make_value("v"));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		assert_eq!(txn.store_reads(), 0);

		// A write is a pure overlay operation: neither it nor its overlay-served read-back counts.
		let pending_key = make_key("pending");
		txn.state_set(operator_id, &pending_key, make_value("p")).unwrap();
		assert_eq!(txn.state_get(operator_id, &pending_key).unwrap(), Some(make_value("p")));
		assert_eq!(txn.store_reads(), 0, "overlay-served reads must not count as store reads");

		// A committed row and a miss both reach the store: one point get each.
		assert!(txn.state_get(operator_id, &committed_key).unwrap().is_some());
		assert_eq!(txn.store_reads(), 1);
		assert!(txn.state_get(operator_id, &make_key("absent")).unwrap().is_none());
		assert_eq!(txn.store_reads(), 2, "a store-reaching miss is still a store read");

		// Batched reads count per external key, not per call.
		let batch = txn.state_get_many(operator_id, &[make_key("absent_a"), make_key("absent_b")]).unwrap();
		assert!(batch.items.is_empty());
		assert_eq!(txn.store_reads(), 4);

		// State writes carry their own anchors, so no write ever reads the prior row back.
		let wide_key = make_key("wide");
		txn.state_set(operator_id, &wide_key, EncodedRow(CowVec::new(vec![0u8; 32]))).unwrap();
		assert_eq!(txn.store_reads(), 4, "a state write must never reach the store");
	}

	#[test]
	fn state_reads_are_cached_within_a_transaction() {
		// The snapshot is immutable within a txn, so a second read of the same key (hit or miss,
		// point or batch) must come from the read-through cache. Without it every operator re-pays
		// a store roundtrip for state it already loaded in the same slice.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let committed_key = make_key("committed");
		commit_state_row(&engine, operator_id, &committed_key, make_value("v"));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert_eq!(txn.state_get(operator_id, &committed_key).unwrap(), Some(make_value("v")));
		assert_eq!(txn.store_reads(), 1);
		assert_eq!(txn.state_get(operator_id, &committed_key).unwrap(), Some(make_value("v")));
		assert_eq!(txn.store_reads(), 1, "a repeated state read must be served from the cache");

		assert_eq!(txn.state_get(operator_id, &make_key("absent")).unwrap(), None);
		assert_eq!(txn.store_reads(), 2);
		assert_eq!(txn.state_get(operator_id, &make_key("absent")).unwrap(), None);
		assert_eq!(txn.store_reads(), 2, "a miss must be cached too, or absent-key probes re-scan forever");

		// The batch path shares the cache in both directions: prior point reads are not
		// re-fetched, and batch-fetched keys serve later point reads.
		let batch = txn
			.state_get_many(
				operator_id,
				&[committed_key.clone(), make_key("absent"), make_key("batch_only")],
			)
			.unwrap();
		assert_eq!(batch.items.len(), 1);
		assert_eq!(batch.items[0].row, make_value("v"));
		assert_eq!(txn.store_reads(), 3, "only the never-read key may reach the store");
		assert_eq!(txn.state_get(operator_id, &make_key("batch_only")).unwrap(), None);
		assert_eq!(txn.store_reads(), 3);
	}

	#[test]
	fn cached_state_reads_never_mask_writes_or_removes() {
		// The cache sits below the pending overlays, so a write or remove issued after a cached read
		// wins on every later read. Consulting the cache first would let an operator read back its
		// own stale pre-write state and fold updates into a dead accumulator.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let key = make_key("k");
		commit_state_row(&engine, operator_id, &key, make_value("old"));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert_eq!(txn.state_get(operator_id, &key).unwrap(), Some(make_value("old")));
		txn.state_set(operator_id, &key, make_value("new")).unwrap();
		assert_eq!(txn.state_get(operator_id, &key).unwrap(), Some(make_value("new")));

		txn.state_remove(operator_id, &key).unwrap();
		assert_eq!(txn.state_get(operator_id, &key).unwrap(), None);
		let batch = txn.state_get_many(operator_id, &[key.clone()]).unwrap();
		assert!(batch.items.is_empty(), "a removed key must not resurface through the batch path");

		// A key first seen as a cached miss must surface a later write.
		let fresh = make_key("fresh");
		assert_eq!(txn.state_get(operator_id, &fresh).unwrap(), None);
		txn.state_set(operator_id, &fresh, make_value("live")).unwrap();
		assert_eq!(txn.state_get(operator_id, &fresh).unwrap(), Some(make_value("live")));
	}

	#[test]
	fn a_state_write_keeps_the_callers_anchors_without_reading_the_prior_row() {
		// Operator state rows carry the anchors their writer stamped, so a save is a pure overlay
		// op. Reading the prior row back to carry created_at forward costs a store read per written
		// key per flush, and defeats the caches above once one of them serves the load.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let key = make_key("acc");
		commit_state_row(&engine, operator_id, &key, anchored_row(b"v0", 1_000, 1_000));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert!(txn.state_get(operator_id, &key).unwrap().is_some());
		assert_eq!(txn.store_reads(), 1);
		txn.state_set(operator_id, &key, anchored_row(b"v1", 5_000, 5_000)).unwrap();
		assert_eq!(txn.store_reads(), 1, "a save must not read the prior row back");

		let stored = txn.state_get(operator_id, &key).unwrap().unwrap();
		assert_eq!(
			stored.created_at(),
			DateTime::from_nanos(5_000),
			"the write's own created_at stands: nothing is carried over from the prior row"
		);
		assert_eq!(stored.updated_at(), DateTime::from_nanos(5_000));
	}

	#[test]
	fn deferred_read_sees_state_committed_above_object_version() {
		// A prior consume commits its join state at that consume's commit version, strictly above
		// any input data version. Bounding operator-state reads to the later consume's own object
		// version would hide the other side of the join and emit an unmatched none result.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let inner_key = make_key("late_right_side");
		let value = make_value("matched_row");

		// Two further commits push the operator-state write more than one version above the object
		// version, so the read bound (object_version + 1) cannot reach it on its own.
		let object_version = commit_state_row(&engine, operator_id, &make_key("warmup_a"), make_value("a"));
		commit_state_row(&engine, operator_id, &make_key("warmup_b"), make_value("b"));
		let committed_at = commit_state_row(&engine, operator_id, &inner_key, value.clone());
		assert!(
			committed_at.0 >= object_version.0 + 2,
			"operator state must commit at least two versions above the object version: committed_at={committed_at:?} object_version={object_version:?}"
		);

		let (state_version, lease) = engine.acquire_current_snapshot_lease().unwrap();
		assert!(state_version >= committed_at);

		let query = engine.multi().begin_query_at_version(&lease).unwrap();
		let state_query = engine.multi().begin_query_at_version(&lease).unwrap();
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: object_version,
			pending: Pending::new(),
			base_pending: PendingLayers::empty(),
			query,
			state_query,
			single: engine.single().clone(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: FlowSubstrate::new(),
			state_budget: OperatorStateBudgetHandle::default(),
		});

		let batch = txn.state_get_many(operator_id, &[inner_key]).unwrap();
		assert_eq!(
			batch.items.len(),
			1,
			"operator state committed at {committed_at:?} (above object_version {object_version:?}) must be visible to a deferred read"
		);
		assert_eq!(batch.items[0].row, value);
	}

	#[test]
	fn committing_persists_state_writes_and_keeps_prior_state() {
		// The committing variant wraps the command being committed, so state writes route to that
		// command rather than the in-memory pending and become durable when the flow commits,
		// alongside state committed by prior transactions.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let prior_key = make_key("prior");
		let prior_value = make_value("prior_value");
		commit_state_row(&engine, operator_id, &prior_key, prior_value.clone());

		let written_key = make_key("written_by_tick");
		let written_value = make_value("tick_value");
		{
			let cmd = engine.begin_command(IdentityId::system()).unwrap();
			let mut txn = FlowTransaction::committing(CommittingParams {
				cmd,
				catalog: Catalog::testing(),
				interceptors: engine.create_interceptors(),
				clock: engine.clock().clone(),
				substrate: FlowSubstrate::new(),
				state_budget: OperatorStateBudgetHandle::default(),
			})
			.unwrap();
			txn.state_set(operator_id, &written_key, written_value.clone()).unwrap();
			txn.commit().unwrap();
		}

		let (_version, lease) = engine.acquire_current_snapshot_lease().unwrap();
		let query = engine.multi().begin_query_at_version(&lease).unwrap();
		let prior_encoded = OperatorStateKey::encoded(operator_id, prior_key.as_slice());
		let written_encoded = OperatorStateKey::encoded(operator_id, written_key.as_slice());
		let found = query.get_many(&[prior_encoded.clone(), written_encoded.clone()]).unwrap();
		assert_eq!(
			found.len(),
			2,
			"the committing flow's write and the prior committed state must both be durable after commit"
		);
		assert_eq!(found.get(&prior_encoded).unwrap().row, prior_value);
		assert_eq!(found.get(&written_encoded).unwrap().row, written_value);
	}

	#[test]
	fn transactional_read_sees_committed_state_below_version_and_base_pending() {
		// The transactional variant reads committed state via state_query at the latest snapshot
		// plus a base_pending overlay. The read must not be bounded to the txn `version`, which is
		// set below the committed state here and would hide it.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let committed_key = make_key("committed");
		let committed_value = make_value("committed_value");

		let low_version = commit_state_row(&engine, operator_id, &make_key("warmup"), make_value("w"));
		commit_state_row(&engine, operator_id, &make_key("bump"), make_value("bump"));
		let committed_at = commit_state_row(&engine, operator_id, &committed_key, committed_value.clone());
		assert!(
			committed_at.0 >= low_version.0 + 2,
			"committed state must land at least two versions above the txn version so a wrongful bound (which resolves to version + 1) would hide it: committed_at={committed_at:?} low_version={low_version:?}"
		);

		let base_key = make_key("in_flight");
		let base_value = make_value("in_flight_value");
		let mut base_pending = Pending::new();
		base_pending.insert(OperatorStateKey::encoded(operator_id, base_key.as_slice()), base_value.clone());

		let mut txn = FlowTransaction::transactional(TransactionalParams {
			version: low_version,
			pending: Pending::new(),
			base_pending,
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			single: engine.single().clone(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			view_overlay: Arc::new(Vec::new()),
			substrate: FlowSubstrate::new(),
			state_budget: OperatorStateBudgetHandle::default(),
		});

		let committed = txn.state_get_many(operator_id, &[committed_key]).unwrap();
		assert_eq!(
			committed.items.len(),
			1,
			"committed state at {committed_at:?} must be visible even though the txn version is {low_version:?}"
		);
		assert_eq!(committed.items[0].row, committed_value);

		let base = txn.state_get_many(operator_id, &[base_key]).unwrap();
		assert_eq!(base.items.len(), 1);
		assert_eq!(base.items[0].row, base_value);
	}

	#[test]
	fn deferred_read_sees_base_pending_overlay() {
		// The pinned query snapshot cannot see the flow's last commit, so a deferred slice reads its
		// own prior writes through the base_pending overlay.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);

		let committed_key = make_key("committed");
		let committed_value = make_value("committed_value");
		let low_version = commit_state_row(&engine, operator_id, &make_key("warmup"), make_value("w"));
		commit_state_row(&engine, operator_id, &committed_key, committed_value.clone());

		let overlaid_key = make_key("overlaid");
		let overlaid_value = make_value("overlaid_value");
		let mut base_pending = Pending::new();
		base_pending.insert(
			OperatorStateKey::encoded(operator_id, overlaid_key.as_slice()),
			overlaid_value.clone(),
		);
		base_pending.remove(OperatorStateKey::encoded(operator_id, committed_key.as_slice()));

		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: low_version,
			pending: Pending::new(),
			base_pending: PendingLayers::single(Arc::new(base_pending)),
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			single: engine.single().clone(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: FlowSubstrate::new(),
			state_budget: OperatorStateBudgetHandle::default(),
		});

		assert_eq!(
			txn.state_get(operator_id, &overlaid_key).unwrap(),
			Some(overlaid_value.clone()),
			"a Set in base_pending must resolve through the overlay"
		);
		assert_eq!(
			txn.state_get(operator_id, &committed_key).unwrap(),
			None,
			"a Remove in base_pending must shadow the committed row"
		);

		let batch = txn.state_get_many(operator_id, &[overlaid_key.clone(), committed_key.clone()]).unwrap();
		assert_eq!(batch.items.len(), 1);
		assert_eq!(batch.items[0].row, overlaid_value);

		let scan = txn.state_scan_all(operator_id).unwrap();
		let scanned: Vec<_> = scan.items.iter().map(|item| item.row.clone()).collect();
		assert!(scanned.contains(&overlaid_value), "range merge must surface base_pending Sets");
		assert!(!scanned.contains(&committed_value), "range merge must shadow base_pending Removes");

		let shadow_value = make_value("shadow");
		txn.state_set(operator_id, &overlaid_key, shadow_value.clone()).unwrap();
		assert_eq!(txn.state_get(operator_id, &overlaid_key).unwrap(), Some(shadow_value));
	}

	#[test]
	fn deferred_reads_owned_rows_at_state_version() {
		// After a restart a flow's own materialized rows sit above the version its next slice pins
		// `query` to, with an empty overlay, so owned-row keys must route through state_query at
		// the lease. Ephemeral stays pinned because subscription hydration reads as-of a version.
		let engine = TestEngine::new();
		let row_key = RowKey::encoded(StorageId::table(TableId(7)), RowNumber(1));
		let row_value = make_value("own_row");

		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		cmd.set(&make_key("warmup").into_encoded(), make_value("w")).unwrap();
		let low_version = cmd.commit_unchecked().unwrap();

		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		cmd.set(&row_key, row_value.clone()).unwrap();
		let committed_at = cmd.commit_unchecked().unwrap();
		assert!(low_version < committed_at);

		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: low_version,
			pending: Pending::new(),
			base_pending: PendingLayers::empty(),
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			single: engine.single().clone(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: FlowSubstrate::new(),
			state_budget: OperatorStateBudgetHandle::default(),
		});
		assert_eq!(
			txn.get(&row_key).unwrap(),
			Some(row_value.clone()),
			"a deferred txn pinned below the flow's own commit must read its rows at the state version"
		);
		assert!(txn.contains_key(&row_key).unwrap());

		let mut ephemeral = FlowTransaction::ephemeral(
			low_version,
			engine.multi().begin_query().unwrap(),
			engine.single().clone(),
			Catalog::testing(),
			HashMap::new(),
			engine.clock().clone(),
			OperatorStateBudgetHandle::default(),
		);
		assert_eq!(
			ephemeral.get(&row_key).unwrap(),
			None,
			"ephemeral (subscription) row reads must stay pinned to the requested version"
		);
	}

	#[test]
	fn ephemeral_read_sees_state_map_and_pending() {
		// The ephemeral variant has no state_query, so it serves operator-state reads from an
		// in-memory state map with the pending overlay on top.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let seeded_key = make_key("seeded");
		let seeded_value = make_value("seeded_value");

		let mut state = HashMap::new();
		state.insert(OperatorStateKey::encoded(operator_id, seeded_key.as_slice()), seeded_value.clone());

		let mut txn = FlowTransaction::ephemeral(
			CommitVersion(1),
			engine.multi().begin_query().unwrap(),
			engine.single().clone(),
			Catalog::testing(),
			state,
			engine.clock().clone(),
			OperatorStateBudgetHandle::default(),
		);

		let seeded = txn.state_get_many(operator_id, &[seeded_key]).unwrap();
		assert_eq!(seeded.items.len(), 1, "seeded ephemeral state must be readable");
		assert_eq!(seeded.items[0].row, seeded_value);

		let live_key = make_key("live");
		let live_value = make_value("live_value");
		txn.state_set(operator_id, &live_key, live_value.clone()).unwrap();
		let live = txn.state_get_many(operator_id, &[live_key]).unwrap();
		assert_eq!(live.items.len(), 1);
		assert_eq!(live.items[0].row, live_value);
	}

	#[test]
	fn batch_prefetch_respects_byte_cap() {
		// fetch_external memoizes what it fetched, so without the cap the batch path reopens the
		// unbounded per-transaction memo growth the point-path cap closed. A rejected entry must
		// not be counted, must not enter the memo, and must not shrink the batch result.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let key = make_key("k1");
		commit_state_row(&engine, operator_id, &key, make_value("v"));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		// Fill the counter to the cap so the next memoization cannot fit.
		txn.inner_mut().prefetch_bytes = PREFETCH_MEMO_BYTE_CAP;

		let batch = txn.state_get_many(operator_id, &[key.clone()]).unwrap();
		assert_eq!(batch.items.len(), 1, "the cap bounds the memo, not the batch result");
		assert_eq!(txn.inner().prefetch_rejections, 1, "an over-cap batch memoization must be rejected");
		assert_eq!(txn.inner().prefetch_bytes, PREFETCH_MEMO_BYTE_CAP, "a rejected entry must not be counted");

		// The rejected entry is not memoized: a re-read reaches the store again.
		let reads_before = txn.store_reads();
		let again = txn.state_get_many(operator_id, &[key]).unwrap();
		assert_eq!(again.items.len(), 1);
		assert_eq!(txn.store_reads(), reads_before + 1, "a rejected entry must not serve later reads");
	}

	#[test]
	fn batch_prefetch_accounts_bytes_like_the_point_path() {
		// prefetch_bytes must reflect the memo no matter which path filled it, or the cap is
		// meaningless for batch-heavy operators.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let hit = make_key("hit");
		let miss = make_key("miss");
		commit_state_row(&engine, operator_id, &hit, make_value("v"));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();

		let mut batch_txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let fetched = batch_txn.state_get_many(operator_id, &[hit.clone(), miss.clone()]).unwrap();
		assert_eq!(fetched.items.len(), 1);
		let batch_bytes = batch_txn.inner().prefetch_bytes;
		assert!(batch_bytes > 0, "batch memoization must be counted");
		assert_eq!(batch_txn.inner().prefetch_rejections, 0);

		let mut point_txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		assert!(point_txn.state_get(operator_id, &hit).unwrap().is_some());
		assert!(point_txn.state_get(operator_id, &miss).unwrap().is_none());
		assert_eq!(
			point_txn.inner().prefetch_bytes,
			batch_bytes,
			"the batch path must account bytes with the exact point-path formula"
		);
	}
}
