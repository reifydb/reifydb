// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::operator_state::{GroupStateKey, OperatorStateKey, node_prefix},
	metrics::scan::ScanCounters,
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{Result, error::Error as ValueError};
use tracing::{Span, field, instrument};

use super::{DepFlowTransaction, substrate::operator_state_coordinates};

impl DepFlowTransaction {
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len(),
		found = field::Empty
	))]
	pub fn state_get(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		let result = match self.scoped_get(id, key)? {
			Some(bytes) => Some(EncodedOperatorRow::try_from(bytes).map_err(ValueError::from)?),
			None => None,
		};
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

	#[instrument(name = "flow::state::set", level = "trace", skip(self, row), fields(
		operator_id = id.0,
		key_len = key.as_slice().len(),
		value_len = row.len()
	))]
	pub fn state_set(&mut self, id: OperatorId, key: &GroupStateKey, row: EncodedOperatorRow) -> Result<()> {
		self.scoped_set(id, key, row.into_bytes())
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
		let prefixed_range = range.with_prefix(EncodedKey::new(node_prefix(id)));
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
		operator_id = id.0,
		site = site,
		rows_fetched = field::Empty,
		rows_tombstoned = field::Empty
	))]
	pub fn state_range(
		&mut self,
		id: OperatorId,
		range: EncodedKeyRange,
		limit: Option<usize>,
		site: &'static str,
	) -> Result<MultiVersionBatch> {
		let before = ScanCounters::sample();
		let prefixed_range = range.with_prefix(EncodedKey::new(node_prefix(id)));
		let iter = self.range(prefixed_range, RangeScope::All, 1024);
		let mut items = Vec::new();
		let mut has_more = false;
		for result in iter {
			if limit.is_some_and(|l| items.len() == l) {
				has_more = true;
				break;
			}
			items.push(result?);
		}
		let scanned = before.since();
		let span = Span::current();
		span.record("rows_fetched", scanned.fetched);
		span.record("rows_tombstoned", scanned.tombstones);
		Ok(MultiVersionBatch {
			items,
			has_more,
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

	fn scoped_get(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedBytes>> {
		self.get(&scoped_key(id, key))
	}

	fn scoped_get_many(&mut self, id: OperatorId, keys: &[GroupStateKey]) -> Result<MultiVersionBatch> {
		let version = self.version();
		let encoded: Vec<EncodedKey> = keys.iter().map(|key| scoped_key(id, key)).collect();

		let mut items: Vec<MultiVersionRow> = Vec::new();
		let mut to_batch: Vec<EncodedKey> = Vec::new();

		for encoded_key in &encoded {
			match self.lookup_overlays(encoded_key) {
				Some(None) => continue,
				Some(Some(bytes)) => items.push(MultiVersionRow {
					key: encoded_key.clone(),
					bytes,
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
	fn lookup_overlays(&self, encoded_key: &EncodedKey) -> Option<Option<EncodedBytes>> {
		let pending = match self {
			Self::Deferred(d) => &d.pending,
			Self::Ephemeral(e) => &e.pending,
		};
		if pending.is_removed(encoded_key) {
			return Some(None);
		}
		pending.get(encoded_key).map(|row| Some(row.clone()))
	}

	#[inline]
	fn fetch_external(&mut self, to_batch: &[EncodedKey], items: &mut Vec<MultiVersionRow>) -> Result<()> {
		if to_batch.is_empty() {
			return Ok(());
		}

		match self {
			Self::Ephemeral(e) => {
				let version = e.version;
				for encoded_key in to_batch {
					if let Some(bytes) = e.state.get(encoded_key) {
						items.push(MultiVersionRow {
							key: encoded_key.clone(),
							bytes: bytes.clone(),
							version,
						});
					}
				}
			}
			Self::Deferred(d) => {
				d.store_reads += to_batch.len() as u64;
				let version = d.version;
				for encoded_key in to_batch {
					let (operator, inner_key) = operator_state_coordinates(encoded_key)
						.expect("state_get_many keys must carry an operator id");
					if let Some(row) = d.substrate.operators.get(operator, &inner_key) {
						items.push(MultiVersionRow {
							key: encoded_key.clone(),
							bytes: row.into_bytes(),
							version,
						});
					}
				}
			}
		}

		Ok(())
	}

	fn scoped_set(&mut self, id: OperatorId, key: &GroupStateKey, value: EncodedBytes) -> Result<()> {
		self.set(&scoped_key(id, key), value)
	}

	fn scoped_remove(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<()> {
		self.remove_silent(&scoped_key(id, key))
	}
}

fn scoped_key(id: OperatorId, key: &GroupStateKey) -> EncodedKey {
	let mut bytes = node_prefix(id);
	bytes.extend_from_slice(key.as_slice());
	EncodedKey::new(bytes)
}

#[cfg(test)]
pub mod tests {
	use std::collections::{Bound, HashMap};

	use reifydb_catalog::catalog::Catalog;
	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		row::bytes::EncodedBytes,
	};
	use reifydb_core::{
		actors::pending::{Pending, PendingLayers},
		common::CommitVersion,
		interface::catalog::{flow::OperatorId, id::TableId, storage::StorageId},
		key::{
			EncodableKey,
			operator_state::{GroupId, Keyspace, OperatorStateKey},
			row::RowKey,
		},
	};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::{datetime::DateTime, identity::IdentityId, row_number::RowNumber};

	use super::*;
	use crate::{
		test_util::create_test_transaction,
		transaction::{DeferredParams, substrate::FlowSubstrate},
	};

	fn seed_state_row(engine: &TestEngine, operator: OperatorId, key: &GroupStateKey, row: EncodedOperatorRow) {
		// Stands in for a prior slice's success-side operator state apply.
		engine.inner().operator_state().set(operator, EncodedKey::new(key.as_slice()), row);
	}

	fn deferred_shared(engine: &TestEngine) -> DepFlowTransaction {
		// Shares the engine's operator state store like every production deferred txn.
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		DepFlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: PendingLayers::empty(),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(1000)),
			substrate: FlowSubstrate {
				operators: engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
		})
	}

	fn make_key(s: &str) -> GroupStateKey {
		// Framed as an operator composes its keys, or these tests would assert against a key reclamation could
		// prefix-delete.
		OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::CUSTOM, s.as_bytes())
	}

	fn make_value(s: &str) -> EncodedOperatorRow {
		EncodedOperatorRow::timeless(s.as_bytes())
	}

	fn full_key(operator: OperatorId, key: &GroupStateKey) -> EncodedKey {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
			.expect("scoped state keys must carry a structured inner encoding");
		OperatorStateKey::encoded(operator, group, keyspace, suffix)
	}

	fn stamped_row(payload: &[u8], time: u64) -> EncodedOperatorRow {
		EncodedOperatorRow::new(payload, DateTime::from_nanos(time))
	}

	#[test]
	fn test_state_get_set() {
		let parent = create_test_transaction();
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut decoded: Vec<(Vec<u8>, EncodedBytes)> = batch
			.items
			.iter()
			.map(|item| {
				(
					OperatorStateKey::decode(&item.key).unwrap().inner().as_slice().to_vec(),
					item.bytes.clone(),
				)
			})
			.collect();
		decoded.sort_by(|a, b| a.0.cmp(&b.0));
		assert_eq!(decoded[0], (make_key("a").as_slice().to_vec(), make_value("data").into_bytes()));
		assert_eq!(decoded[1], (make_key("b").as_slice().to_vec(), make_value("2").into_bytes()));
	}

	#[test]
	fn test_state_get_nonexistent() {
		let parent = create_test_transaction();
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
		let mut txn = DepFlowTransaction::deferred(
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
	fn test_state_multiple_nodes() {
		let parent = create_test_transaction();
		let mut txn = DepFlowTransaction::deferred(
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
		seed_state_row(&engine, operator_id, &committed_key, make_value("v"));

		let mut txn = deferred_shared(&engine);
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

		// State writes carry their own time, so no write ever reads the prior row back.
		let wide_key = make_key("wide");
		txn.state_set(operator_id, &wide_key, EncodedOperatorRow::timeless(&[0u8; 32])).unwrap();
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
		seed_state_row(&engine, operator_id, &committed_key, make_value("v"));

		let mut txn = deferred_shared(&engine);

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
		assert_eq!(batch.items[0].bytes, make_value("v").into_bytes());
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
		seed_state_row(&engine, operator_id, &key, make_value("old"));

		let mut txn = deferred_shared(&engine);

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
	fn a_state_write_keeps_the_callers_time_without_reading_the_prior_row() {
		// A row carries the time its writer stamped, so a save must never read the prior row back.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let key = make_key("acc");
		seed_state_row(&engine, operator_id, &key, stamped_row(b"v0", 1_000));

		let mut txn = deferred_shared(&engine);

		assert!(txn.state_get(operator_id, &key).unwrap().is_some());
		assert_eq!(txn.store_reads(), 1);
		txn.state_set(operator_id, &key, stamped_row(b"v1", 5_000)).unwrap();
		assert_eq!(txn.store_reads(), 1, "a save must not read the prior row back");

		let stored = txn.state_get(operator_id, &key).unwrap().unwrap();
		assert_eq!(
			stored.time(),
			DateTime::from_nanos(5_000),
			"the write's own time stands: nothing is carried over from the prior row"
		);
		assert_eq!(stored.body(), b"v1");
	}

	#[test]
	fn deferred_read_sees_state_committed_above_object_version() {
		// State reads resolve read-latest from the operator state store; bounding them to the pinned object
		// version would hide the other side of a join.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);
		let inner_key = make_key("late_right_side");
		let value = make_value("matched_row");

		// Pinned before the state is applied, so a version-bounded read could not see it.
		let object_version = engine.inner().current_version().unwrap();
		seed_state_row(&engine, operator_id, &make_key("warmup_a"), make_value("a"));
		seed_state_row(&engine, operator_id, &inner_key, value.clone());

		let mut txn = DepFlowTransaction::deferred_from_parts(DeferredParams {
			version: object_version,
			pending: PendingLayers::empty(),
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: FlowSubstrate {
				operators: engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
		});

		let batch = txn.state_get_many(operator_id, &[inner_key]).unwrap();
		assert_eq!(
			batch.items.len(),
			1,
			"operator state applied above object_version {object_version:?} must be visible to a deferred read"
		);
		assert_eq!(batch.items[0].bytes, value.into_bytes());
	}

	#[test]
	fn deferred_read_sees_base_pending_overlay() {
		// base_pending must shadow whatever the operator state store already holds.
		let engine = TestEngine::new();
		let operator_id = OperatorId(1);

		let committed_key = make_key("committed");
		let committed_value = make_value("committed_value");
		let low_version = engine.inner().current_version().unwrap();
		seed_state_row(&engine, operator_id, &committed_key, committed_value.clone());

		let overlaid_key = make_key("overlaid");
		let overlaid_value = make_value("overlaid_value");
		let mut base_pending = Pending::new();
		base_pending.insert(full_key(operator_id, &overlaid_key), overlaid_value.clone().into_bytes());
		base_pending.remove(full_key(operator_id, &committed_key));

		let mut txn = DepFlowTransaction::deferred_from_parts(DeferredParams {
			version: low_version,
			pending: PendingLayers::over(vec![base_pending]),
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: FlowSubstrate {
				operators: engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
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
		assert_eq!(batch.items[0].bytes, overlaid_value.clone().into_bytes());

		let scan = txn.state_scan_all(operator_id).unwrap();
		let scanned: Vec<_> = scan.items.iter().map(|item| item.bytes.clone()).collect();
		assert!(
			scanned.contains(&overlaid_value.clone().into_bytes()),
			"range merge must surface base_pending Sets"
		);
		assert!(
			!scanned.contains(&committed_value.into_bytes()),
			"range merge must shadow base_pending Removes"
		);

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
		let row_value = make_value("own_row").into_bytes();

		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		cmd.set(&make_key("warmup").into_encoded(), make_value("w").into_bytes()).unwrap();
		let low_version = cmd.commit_unchecked().unwrap();

		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		cmd.set(&row_key, row_value.clone()).unwrap();
		let committed_at = cmd.commit_unchecked().unwrap();
		assert!(low_version < committed_at);

		let mut txn = DepFlowTransaction::deferred_from_parts(DeferredParams {
			version: low_version,
			pending: PendingLayers::empty(),
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: FlowSubstrate::new(),
		});
		assert_eq!(
			txn.get(&row_key).unwrap(),
			Some(row_value.clone()),
			"a deferred txn pinned below the flow's own commit must read its rows at the state version"
		);
		assert!(txn.contains_key(&row_key).unwrap());

		let mut ephemeral = DepFlowTransaction::ephemeral(
			low_version,
			engine.multi().begin_query().unwrap(),
			Catalog::testing(),
			HashMap::new(),
			engine.clock().clone(),
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
		state.insert(full_key(operator_id, &seeded_key), seeded_value.clone().into_bytes());

		let mut txn = DepFlowTransaction::ephemeral(
			CommitVersion(1),
			engine.multi().begin_query().unwrap(),
			Catalog::testing(),
			state,
			engine.clock().clone(),
		);

		let seeded = txn.state_get_many(operator_id, &[seeded_key]).unwrap();
		assert_eq!(seeded.items.len(), 1, "seeded ephemeral state must be readable");
		assert_eq!(seeded.items[0].bytes, seeded_value.into_bytes());

		let live_key = make_key("live");
		let live_value = make_value("live_value");
		txn.state_set(operator_id, &live_key, live_value.clone()).unwrap();
		let live = txn.state_get_many(operator_id, &[live_key]).unwrap();
		assert_eq!(live.items.len(), 1);
		assert_eq!(live.items[0].bytes, live_value.into_bytes());
	}
}
