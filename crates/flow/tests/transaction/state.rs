// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow},
};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers},
	common::CommitVersion,
	interface::catalog::{flow::OperatorId, id::TableId, storage::StorageId},
	key::{
		EncodableKey,
		operator::state::{GroupStateKey, OperatorStateKey, custom_not_cached_key},
		row::RowKey,
	},
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	state::{StateExtension, StateRange},
	substrate::{FlowSubstrate, classify_pending, operator_writes},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_store_operator::types::{DurablePre, OperatorWrite};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	byte_size::ByteSize,
	value::{identity::IdentityId, row_number::RowNumber},
};

use crate::common::create_test_transaction;

fn seed_state_row(engine: &TestEngine, operator: OperatorId, key: &GroupStateKey, row: EncodedPodRow) {
	// Stands in for a prior slice's success-side operator state apply.
	let store = engine.inner().operator_state();
	let key = EncodedKey::new(key.as_slice());
	let write = match store.get(operator, &key) {
		Some(pre) => OperatorWrite::Replace {
			operator,
			key,
			pre_value_bytes: ByteSize::from_bytes(pre.bytes().len() as u64),
			post: row,
		},
		None => OperatorWrite::Insert {
			operator,
			key,
			post: row,
		},
	};
	store.apply_batch(&[write]);
}

fn deferred_shared(engine: &TestEngine) -> DeferredTransaction {
	// Shares the engine's operator state store like every production deferred txn.
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: Some(parent.multi.begin_query().unwrap()),
		state_query: Some(parent.multi.begin_query().unwrap()),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(1000)),
		substrate: FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		),
	})
}

fn make_key(s: &str) -> GroupStateKey {
	// Framed as an operator composes its keys, or these tests would assert against a key reclamation could
	// prefix-delete.
	custom_not_cached_key(s.as_bytes()).expect("a fixture name must fit the keyspace's id width")
}

fn make_value(s: &str) -> EncodedPodRow {
	EncodedPodRow::new(s.as_bytes())
}

fn full_key(operator: OperatorId, key: &GroupStateKey) -> EncodedKey {
	let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
		.expect("scoped state keys must carry a structured inner encoding");
	OperatorStateKey::encoded(operator, group, keyspace, suffix)
}

fn stamped_row(payload: &[u8]) -> EncodedPodRow {
	EncodedPodRow::new(payload)
}

#[test]
fn test_state_get_set() {
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

	let operator_id = OperatorId(1);
	let key = make_key("state_key");
	let value = make_value("state_value");

	txn.state_set(operator_id, &key, value.clone()).unwrap();

	let result = txn.state_get(operator_id, &key).unwrap();
	assert_eq!(result, Some(value));
}

#[test]
fn test_state_get_many() {
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

	let operator_id = OperatorId(1);
	txn.state_set(operator_id, &make_key("a"), make_value("1")).unwrap();
	txn.state_set(operator_id, &make_key("b"), make_value("2")).unwrap();

	// One namespace, so re-writing a key resolves to the latest value; re-splitting the
	// envelopes would return two rows for "a" here.
	txn.state_set(operator_id, &make_key("a"), make_value("data")).unwrap();

	let batch = txn.state_get_many(operator_id, &[make_key("a"), make_key("b"), make_key("missing")]).unwrap();

	// A key with no value is omitted rather than returned empty.
	assert_eq!(batch.items.len(), 2);
	let mut decoded: Vec<(Vec<u8>, EncodedBytes)> = batch
		.items
		.iter()
		.map(|item| {
			(OperatorStateKey::decode(&item.key).unwrap().inner().as_slice().to_vec(), item.bytes.clone())
		})
		.collect();
	decoded.sort_by(|a, b| a.0.cmp(&b.0));
	assert_eq!(decoded[0], (make_key("a").as_slice().to_vec(), make_value("data").into_bytes()));
	assert_eq!(decoded[1], (make_key("b").as_slice().to_vec(), make_value("2").into_bytes()));
}

#[test]
fn test_state_get_nonexistent() {
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

	let operator_id = OperatorId(1);
	let key = make_key("missing");

	let result = txn.state_get(operator_id, &key).unwrap();
	assert_eq!(result, None);
}

#[test]
fn test_state_remove() {
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

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
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

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
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

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
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

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
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

	let operator_id = OperatorId(1);

	let iter = txn.state_scan_all(operator_id).unwrap();
	assert!(iter.items.into_iter().next().is_none());
}

#[test]
fn test_state_range_all() {
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

	let operator_id = OperatorId(1);

	txn.state_set(operator_id, &make_key("a"), make_value("1")).unwrap();
	txn.state_set(operator_id, &make_key("b"), make_value("2")).unwrap();
	txn.state_set(operator_id, &make_key("c"), make_value("3")).unwrap();
	txn.state_set(operator_id, &make_key("d"), make_value("4")).unwrap();

	let range = EncodedKeyRange::new(
		Bound::Included(make_key("b").into_encoded()),
		Bound::Excluded(make_key("d").into_encoded()),
	);
	let iter = txn.state_range(operator_id, StateRange::forward(range, "test")).unwrap();
	let items: Vec<_> = iter.items.into_iter().collect();

	assert_eq!(items.len(), 2);
}

#[test]
fn test_state_clear() {
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

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
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

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
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

	let operator_id = OperatorId(1);

	txn.state_clear(operator_id).unwrap();
}

#[test]
fn test_state_multiple_nodes() {
	let (parent, operators) = create_test_transaction();
	let mut txn = DeferredTransaction::new(DeferredParams::from_parent(
		&parent,
		operators,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	));

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
fn a_state_write_replaces_the_seeded_row_wholesale() {
	// A state_set over a seeded row must replace it wholesale, or a merging write would leave the seeded body
	// readable.
	let engine = TestEngine::new();
	let operator_id = OperatorId(1);
	let key = make_key("acc");
	seed_state_row(&engine, operator_id, &key, stamped_row(b"v0"));

	let mut txn = deferred_shared(&engine);

	assert!(txn.state_get(operator_id, &key).unwrap().is_some());
	txn.state_set(operator_id, &key, stamped_row(b"v1")).unwrap();

	let stored = txn.state_get(operator_id, &key).unwrap().unwrap();
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

	let mut txn = DeferredTransaction::new(DeferredParams {
		version: object_version,
		pending: PendingLayers::empty(),
		query: Some(engine.multi().begin_query().unwrap()),
		state_query: Some(engine.multi().begin_query().unwrap()),
		catalog: Catalog::testing(),
		interceptors: engine.create_interceptors(),
		clock: engine.clock().clone(),
		substrate: FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		),
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

	let mut txn = DeferredTransaction::new(DeferredParams {
		version: low_version,
		pending: PendingLayers::over(vec![base_pending]),
		query: Some(engine.multi().begin_query().unwrap()),
		state_query: Some(engine.multi().begin_query().unwrap()),
		catalog: Catalog::testing(),
		interceptors: engine.create_interceptors(),
		clock: engine.clock().clone(),
		substrate: FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		),
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
	assert!(scanned.contains(&overlaid_value.clone().into_bytes()), "range merge must surface base_pending Sets");
	assert!(!scanned.contains(&committed_value.into_bytes()), "range merge must shadow base_pending Removes");

	let shadow_value = make_value("shadow");
	txn.state_set(operator_id, &overlaid_key, shadow_value.clone()).unwrap();
	assert_eq!(txn.state_get(operator_id, &overlaid_key).unwrap(), Some(shadow_value));
}

#[test]
fn deferred_reads_owned_rows_at_state_version() {
	// A restart puts a flow's own rows above the version its next slice pins, so owned-row keys must route through
	// state_query.
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

	let mut txn = DeferredTransaction::new(DeferredParams {
		version: low_version,
		pending: PendingLayers::empty(),
		query: Some(engine.multi().begin_query().unwrap()),
		state_query: Some(engine.multi().begin_query().unwrap()),
		catalog: Catalog::testing(),
		interceptors: engine.create_interceptors(),
		clock: engine.clock().clone(),
		substrate: FlowSubstrate::new(engine.inner().dictionary_allocators()),
	});
	assert_eq!(
		txn.get(&row_key).unwrap(),
		Some(row_value.clone()),
		"a deferred txn pinned below the flow's own commit must read its rows at the state version"
	);
	assert!(txn.contains_key(&row_key).unwrap());
}

#[test]
fn a_pending_remove_hides_the_stored_row_it_deleted_from_the_last_read() {
	// The flow layer merges its pending writes over the store the same way the store merges buffer over
	// sqlite, and it has the same resurrection hazard: a pending Remove on the greatest key must not let
	// the store's copy of that key answer as the last row. The overlay has to keep walking down, and it
	// must re-ask the store below the suppressed key rather than reuse the candidate it already holds.
	let engine = TestEngine::new();
	let operator = OperatorId(1);
	seed_state_row(&engine, operator, &make_key("a"), make_value("low"));
	seed_state_row(&engine, operator, &make_key("b"), make_value("high"));
	let range = || {
		EncodedKeyRange::new(
			Bound::Included(make_key("a").into_encoded()),
			Bound::Included(make_key("z").into_encoded()),
		)
	};

	let mut txn = deferred_shared(&engine);
	assert_eq!(
		txn.state_last(operator, range()).unwrap().map(|r| r.key),
		Some(full_key(operator, &make_key("b"))),
		"both rows must start out visible"
	);

	txn.state_remove(operator, &make_key("b")).unwrap();

	assert_eq!(
		txn.state_last(operator, range()).unwrap().map(|r| r.key),
		Some(full_key(operator, &make_key("a"))),
		"the pending remove on the greatest key must fall through to the next stored row below it"
	);

	txn.state_remove(operator, &make_key("a")).unwrap();

	assert_eq!(
		txn.state_last(operator, range()).unwrap().map(|r| r.key),
		None,
		"a range whose every stored row is pending-removed has no last row"
	);
}

#[test]
fn a_deferred_state_write_still_classifies_against_the_durable_pre_image() {
	// The write path no longer reads a pre-image; classify_pending resolves every unclassified key in one
	// batch at drain. A key that is already durable must still emit Replace carrying its exact byte size,
	// and an unseen key must emit Insert. Falsified by returning an empty classification (operator_writes
	// panics on unclassified) or by claiming Absent for the seeded key, which turns the Replace into an
	// Insert and drifts the census by the pre-image size forever.
	let engine = TestEngine::new();
	let operator = OperatorId(1);
	let seeded = make_key("durable");
	let fresh = make_key("fresh");

	seed_state_row(&engine, operator, &seeded, make_value("0123456789"));

	let store = engine.inner().operator_state();
	let seeded_inner = EncodedKey::new(seeded.as_slice());
	let fresh_inner = EncodedKey::new(fresh.as_slice());
	let expected_pre = ByteSize::from_bytes(
		store.get(operator, &seeded_inner).expect("seeded row must be durable before the write").bytes().len()
			as u64,
	);

	let mut txn = deferred_shared(&engine);
	txn.state_set(operator, &seeded, make_value("replacement")).unwrap();
	txn.state_set(operator, &fresh, make_value("new")).unwrap();

	let pending = txn.take_pending();
	let deferred = classify_pending(&store, &pending);
	let writes = operator_writes(&pending, &deferred);

	let replace = writes
		.iter()
		.find_map(|write| match write {
			OperatorWrite::Replace {
				key,
				pre_value_bytes,
				..
			} if *key == seeded_inner => Some(*pre_value_bytes),
			_ => None,
		})
		.expect("a write over a durable row must classify as Replace");
	assert_eq!(
		replace, expected_pre,
		"the deferred classification must carry the durable pre-image size, not a guess"
	);

	assert!(
		writes.iter().any(|write| matches!(write, OperatorWrite::Insert { key, .. } if *key == fresh_inner)),
		"a write over a key with no durable row must classify as Insert"
	);
	assert!(
		!writes.iter().any(|write| matches!(write, OperatorWrite::Replace { key, .. } if *key == fresh_inner)),
		"an absent key must never be claimed as Replace"
	);
}

#[test]
fn clearing_state_claims_a_pre_image_only_for_the_keys_the_store_actually_holds() {
	// state_clear scans through txn.range, which merges the pending overlay, so a key this transaction has
	// already written is measured at the size of its own uncommitted value. Claiming that size debits the
	// census for a row the store never held, so such a key must reach the drain unclassified and let the
	// deferred probe resolve it as absent. Falsified by classifying every scanned key, which turns the fresh
	// key's removal into Present and drifts the bucket until the next restart.
	let engine = TestEngine::new();
	let operator = OperatorId(1);
	let durable = make_key("durable");
	let fresh = make_key("fresh");

	seed_state_row(&engine, operator, &durable, make_value("0123456789"));

	let store = engine.inner().operator_state();
	let durable_inner = EncodedKey::new(durable.as_slice());
	let fresh_inner = EncodedKey::new(fresh.as_slice());
	let expected_pre = ByteSize::from_bytes(
		store.get(operator, &durable_inner)
			.expect("the seeded row must be durable before the clear")
			.bytes()
			.len() as u64,
	);

	let mut txn = deferred_shared(&engine);
	txn.state_set(operator, &fresh, make_value("new")).unwrap();
	txn.state_clear(operator).unwrap();

	let pending = txn.take_pending();
	let deferred = classify_pending(&store, &pending);
	let writes = operator_writes(&pending, &deferred);

	let pre_of = |wanted: &EncodedKey| {
		writes.iter()
			.find_map(|write| match write {
				OperatorWrite::Remove {
					key,
					pre,
					..
				} if key == wanted => Some(*pre),
				_ => None,
			})
			.expect("state_clear must emit a Remove for every key its scan reached")
	};

	assert_eq!(
		pre_of(&durable_inner),
		DurablePre::Present(expected_pre),
		"a key the store holds must be removed against its exact durable size"
	);
	assert_eq!(
		pre_of(&fresh_inner),
		DurablePre::Absent,
		"a key that exists only as this transaction's own write must never be removed as Present"
	);
}
