// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	actors::pending::PendingLayers,
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, GroupStateKey, Keyspace},
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	state::StateExtension,
	substrate::{FlowSubstrate, apply_operator_state},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::value::identity::IdentityId;

const NODE: OperatorId = OperatorId(1);

fn deferred(engine: &TestEngine) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: parent.multi.begin_query().unwrap(),
		state_query: parent.multi.begin_query().unwrap(),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(0)),
		substrate: FlowSubstrate {
			operators: Some(engine.inner().operator_state()),
			..FlowSubstrate::default()
		},
	})
}

fn commit_pending(engine: &TestEngine, txn: &mut DeferredTransaction) {
	let pending = txn.take_pending();
	apply_operator_state(&engine.inner().operator_state(), &pending);
}

fn key(keyspace: Keyspace, suffix: &str) -> GroupStateKey {
	GroupStateKey::new(GroupId::ROOT, keyspace, suffix.as_bytes())
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn served(txn: &DeferredTransaction) -> (u64, u64) {
	txn.substrate().memo.counters()
}

#[test]
fn a_repeated_read_of_a_memoized_keyspace_reaches_the_store_once() {
	// The whole point of the memo: 30k reads of one immutable schema row must cost one store read.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let k = key(Keyspace::JOIN_SCHEMA, "shape");
	txn.state_set(NODE, &k, row("fields")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut second = deferred(&engine);
	let start = served(&second);
	second.state_get(NODE, &k).unwrap();
	let after_first = served(&second);
	for _ in 0..20 {
		second.state_get(NODE, &k).unwrap();
	}
	let after_repeats = served(&second);

	assert_eq!(
		(after_first.0 - start.0, after_first.1 - start.1),
		(0, 1),
		"the first read must miss the memo and reach the store"
	);
	assert_eq!(
		(after_repeats.0 - after_first.0, after_repeats.1 - after_first.1),
		(20, 0),
		"every later read must be served from the memo"
	);
}

#[test]
fn a_repeated_read_of_an_unlisted_keyspace_always_reaches_the_store() {
	// The allowlist is the safety boundary; a mutable keyspace slipping in would serve stale rows.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let k = key(Keyspace::ACCUMULATOR, "acc");
	txn.state_set(NODE, &k, row("value")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut second = deferred(&engine);
	let start = served(&second);
	for _ in 0..5 {
		second.state_get(NODE, &k).unwrap();
	}

	assert_eq!(served(&second), start, "an unlisted keyspace must never reach the memo at all");
	assert!(second.substrate().memo.is_empty(), "and must never be remembered");
}

#[test]
fn a_write_makes_the_next_read_see_the_new_value() {
	// A memo that survived its own write would hand back the pre-write row forever.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let k = key(Keyspace::GROUP_DICTIONARY, "g");
	txn.state_set(NODE, &k, row("before")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut second = deferred(&engine);
	assert_eq!(second.state_get(NODE, &k).unwrap(), Some(row("before")));
	second.state_set(NODE, &k, row("after")).unwrap();

	assert_eq!(second.state_get(NODE, &k).unwrap(), Some(row("after")), "the write must invalidate the memo");
}

#[test]
fn a_remove_makes_the_next_read_see_absence() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let k = key(Keyspace::GROUP_DICTIONARY, "g");
	txn.state_set(NODE, &k, row("present")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut second = deferred(&engine);
	assert_eq!(second.state_get(NODE, &k).unwrap(), Some(row("present")));
	second.state_remove(NODE, &k).unwrap();

	assert_eq!(second.state_get(NODE, &k).unwrap(), None, "the remove must invalidate the memo");
}

#[test]
fn a_cached_absence_is_corrected_by_the_write_that_follows_it() {
	// set_row_shape probes before writing; a sticky negative would make the shape unreadable.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let k = key(Keyspace::JOIN_SCHEMA, "unseen");

	assert_eq!(txn.state_get(NODE, &k).unwrap(), None);
	let start = served(&txn);
	txn.state_get(NODE, &k).unwrap();
	let after = served(&txn);
	assert_eq!((after.0 - start.0, after.1 - start.1), (1, 0), "a known-absent key must not be probed again");

	txn.state_set(NODE, &k, row("fields")).unwrap();
	assert_eq!(txn.state_get(NODE, &k).unwrap(), Some(row("fields")), "the write must clear the cached absence");
}

#[test]
fn get_many_serves_memoized_keys_without_touching_the_store() {
	// intern_groups resolves through get_many; leaving that path unmemoized keeps 56% of the reads.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let first = key(Keyspace::GROUP_DICTIONARY, "a");
	let second_key = key(Keyspace::GROUP_DICTIONARY, "b");
	txn.state_set(NODE, &first, row("1")).unwrap();
	txn.state_set(NODE, &second_key, row("2")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut reader = deferred(&engine);
	let keys = vec![first.clone(), second_key.clone()];
	reader.state_get_many(NODE, &keys).unwrap();
	let start = served(&reader);
	reader.state_get_many(NODE, &keys).unwrap();
	let after = served(&reader);

	assert_eq!(
		(after.0 - start.0, after.1 - start.1),
		(2, 0),
		"a second get_many over the same keys must be served entirely from the memo"
	);
	let batch = reader.state_get_many(NODE, &keys).unwrap();
	assert_eq!(batch.items.len(), 2, "memoized rows must still be returned, not silently dropped");
}

#[test]
fn get_many_remembers_absence_so_the_next_pass_skips_the_store() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let missing = key(Keyspace::GROUP_DICTIONARY, "never-written");
	let keys = vec![missing.clone()];

	txn.state_get_many(NODE, &keys).unwrap();
	let start = served(&txn);
	txn.state_get_many(NODE, &keys).unwrap();
	let after = served(&txn);

	assert_eq!(
		(after.0 - start.0, after.1 - start.1),
		(1, 0),
		"an absence learned by get_many must be remembered like a value"
	);
	assert!(txn.state_get_many(NODE, &keys).unwrap().items.is_empty(), "an absent key must stay absent");
}

#[test]
fn clearing_the_memo_sends_the_next_read_back_to_the_store() {
	// The batch boundary clear is what keeps a memoized row from outliving the batch that read it.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let k = key(Keyspace::JOIN_SCHEMA, "shape");
	txn.state_set(NODE, &k, row("fields")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut second = deferred(&engine);
	second.state_get(NODE, &k).unwrap();
	second.substrate().memo.clear();
	let start = served(&second);
	second.state_get(NODE, &k).unwrap();
	let after = served(&second);

	assert_eq!(
		(after.0 - start.0, after.1 - start.1),
		(0, 1),
		"a cleared memo must miss and fall through to the store"
	);
}

#[test]
fn state_clear_drops_memoized_rows_it_removes_behind_the_state_api() {
	// state_clear removes scoped keys directly, so a surviving memo entry would resurrect them.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let k = key(Keyspace::GROUP_DICTIONARY, "g");
	txn.state_set(NODE, &k, row("present")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut second = deferred(&engine);
	assert_eq!(second.state_get(NODE, &k).unwrap(), Some(row("present")));
	second.state_clear(NODE).unwrap();

	assert_eq!(second.state_get(NODE, &k).unwrap(), None, "a cleared operator must not read back through the memo");
}

#[test]
fn the_memo_is_scoped_to_the_operator_that_wrote_the_key() {
	// The memo keys on the scoped key; dropping the operator prefix would leak state between nodes.
	let other = OperatorId(2);
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let k = key(Keyspace::JOIN_SCHEMA, "shape");
	txn.state_set(NODE, &k, row("mine")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut second = deferred(&engine);
	assert_eq!(second.state_get(NODE, &k).unwrap(), Some(row("mine")));

	assert_eq!(second.state_get(other, &k).unwrap(), None, "another operator must not see a memoized row");
}

#[test]
fn an_unrelated_key_in_a_memoized_keyspace_is_not_served_by_a_neighbour() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let a = key(Keyspace::JOIN_SCHEMA, "a");
	let b = key(Keyspace::JOIN_SCHEMA, "b");
	txn.state_set(NODE, &a, row("shape-a")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut second = deferred(&engine);
	assert_eq!(second.state_get(NODE, &a).unwrap(), Some(row("shape-a")));

	assert_eq!(second.state_get(NODE, &b).unwrap(), None, "a memoized key must not answer for its neighbour");
}
