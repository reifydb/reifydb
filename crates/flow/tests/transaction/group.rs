// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::pending::PendingLayers,
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, OperatorStateKey, group_data_inner_range},
	},
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	group::*,
	state::StateExtension,
	substrate::{FlowSubstrate, apply_operator_state},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::value::identity::IdentityId;

const NODE: OperatorId = OperatorId(1);

fn group(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

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
		substrate: FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		),
	})
}

fn commit_pending(engine: &TestEngine, txn: &mut DeferredTransaction) {
	let pending = txn.take_pending();
	apply_operator_state(&engine.inner().operator_state(), &pending);
}

#[test]
fn the_first_group_interns_to_the_first_usable_id() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let (id, is_new) = txn.intern_groups(NODE, &[group("first")]).unwrap().remove(0);

	assert_eq!(id, GroupId::FIRST, "the first group must not take the operator-scope id");
	assert!(!id.is_root());
	assert!(is_new, "a never-seen group must report as newly interned");
}

#[test]
fn a_repeated_group_resolves_to_the_same_id() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let (first, new_first) = txn.intern_groups(NODE, &[group("mint")]).unwrap().remove(0);
	let (second, new_second) = txn.intern_groups(NODE, &[group("mint")]).unwrap().remove(0);

	assert_eq!(first, second, "the same group bytes must always resolve to the same id");
	assert!(new_first);
	assert!(!new_second, "only the first sighting is newly interned");
}

#[test]
fn distinct_groups_get_distinct_ids() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let ids: Vec<GroupId> =
		(0..5).map(|i| txn.intern_groups(NODE, &[group(&format!("g{i}"))]).unwrap().remove(0).0).collect();

	let mut unique = ids.clone();
	unique.sort_unstable();
	unique.dedup();
	assert_eq!(unique.len(), ids.len(), "two groups sharing an id would share a state range");
}

#[test]
fn a_batch_dedupes_repeated_groups_and_reports_one_mint() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let batch = vec![group("a"), group("b"), group("a"), group("b"), group("a")];
	let resolved = txn.intern_groups(NODE, &batch).unwrap();

	assert_eq!(resolved[0].0, resolved[2].0);
	assert_eq!(resolved[0].0, resolved[4].0);
	assert_eq!(resolved[1].0, resolved[3].0);
	assert_ne!(resolved[0].0, resolved[1].0);
	assert_eq!(
		resolved.iter().filter(|(_, is_new)| *is_new).count(),
		2,
		"a batch of two distinct groups must report exactly two mints"
	);
}

#[test]
fn ids_survive_a_restart() {
	let engine = TestEngine::new();
	let before = {
		let mut txn = deferred(&engine);
		let id = txn.intern_groups(NODE, &[group("survivor")]).unwrap().remove(0).0;
		txn.intern_groups(NODE, &[group("other")]).unwrap().remove(0);
		commit_pending(&engine, &mut txn);
		id
	};

	let mut txn = deferred(&engine);
	let (after, is_new) = txn.intern_groups(NODE, &[group("survivor")]).unwrap().remove(0);

	assert_eq!(after, before, "a later transaction must resolve an existing group to its stored id");
	assert!(!is_new, "an existing group must not be reported as newly interned after a restart");
}

#[test]
fn a_reborn_group_never_reuses_the_id_of_the_generation_before_it() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let original = txn.intern_groups(NODE, &[group("reborn")]).unwrap().remove(0).0;
	txn.forget_group(NODE, &group("reborn")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut txn = deferred(&engine);
	let (reborn, is_new) = txn.intern_groups(NODE, &[group("reborn")]).unwrap().remove(0);

	assert!(is_new, "a forgotten group is unknown again and must mint afresh");
	assert_ne!(reborn, original, "a reclaimed id must never be handed back out");
}

#[test]
fn a_forgotten_group_stops_resolving() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	txn.intern_groups(NODE, &[group("gone")]).unwrap().remove(0);
	commit_pending(&engine, &mut txn);

	let mut txn = deferred(&engine);
	txn.forget_group(NODE, &group("gone")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut txn = deferred(&engine);
	assert_eq!(
		txn.lookup_groups(NODE, &[group("gone")]).unwrap().remove(0),
		None,
		"a forgotten group must not resurrect from the store"
	);
}

#[test]
fn forgetting_an_absent_group_reports_that_nothing_was_there() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	assert!(!txn.forget_group(NODE, &group("never-interned")).unwrap());
}

#[test]
fn an_id_resolves_back_to_the_bytes_it_was_interned_from() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let bytes = group("two-address-key");

	let (id, _) = txn.intern_groups(NODE, &[bytes.clone()]).unwrap().remove(0);

	assert_eq!(
		txn.group_bytes(NODE, id).unwrap(),
		Some(bytes),
		"an interned group must be resolvable from its id alone"
	);
}

#[test]
fn the_reverse_record_lives_outside_the_group_data_range() {
	// Floor compaction cancels the group's data keyspaces, so the record must sit outside them.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let bytes = group("outlives-its-data");
	let (id, _) = txn.intern_groups(NODE, &[bytes.clone()]).unwrap().remove(0);

	let batch = txn
		.state_range(NODE, group_data_inner_range(id), None, "test")
		.expect("the group data range must scan");
	for item in batch.items {
		let decoded = OperatorStateKey::decode(&item.key).expect("state keys decode");
		let inner =
			GroupStateKey::from_framed(decoded.inner()).expect("the data range yields framed inner keys");
		txn.state_remove(NODE, &inner).unwrap();
	}

	assert_eq!(
		txn.group_bytes(NODE, id).unwrap(),
		Some(bytes),
		"erasing every data row must not take the record identity depends on"
	);
}

#[test]
fn lookup_does_not_intern() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	assert_eq!(txn.lookup_groups(NODE, &[group("absent")]).unwrap().remove(0), None);

	let (id, is_new) = txn.intern_groups(NODE, &[group("absent")]).unwrap().remove(0);
	assert!(is_new, "the earlier lookup must not have interned the group");
	assert_eq!(id, GroupId::FIRST, "a lookup must not consume an id from the counter");
}

#[test]
fn nodes_intern_independently() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let first = txn.intern_groups(OperatorId(1), &[group("shared")]).unwrap().remove(0).0;
	let second = txn.intern_groups(OperatorId(2), &[group("shared")]).unwrap().remove(0).0;

	assert_eq!(first, second, "each operator numbers its own groups from the same starting point");

	let other = txn.intern_groups(OperatorId(2), &[group("only-on-two")]).unwrap().remove(0).0;
	let mut txn = deferred(&engine);
	assert_eq!(
		txn.lookup_groups(OperatorId(1), &[group("only-on-two")]).unwrap().remove(0),
		None,
		"a group interned on one operator must not resolve on another"
	);
	assert_ne!(other, first);
}
