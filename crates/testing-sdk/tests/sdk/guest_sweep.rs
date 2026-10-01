// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, KeyspaceId, KeyspaceMask, OperatorStateKey},
	state::timer::StateStore,
};
use reifydb_flow_async::{operator::host::TxnHostContext, transaction::FlowTransaction};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::flow::operator::{mount::context::InProcessContext, windowed::guest_as_host::GuestAsHost};
use reifydb_testing_sdk::in_process::transaction::TestFlowTransaction;
use reifydb_value::util::{cowvec::CowVec, hash::Hash128};

const OPERATOR: OperatorId = OperatorId(1);

fn group() -> GroupId {
	GroupId::hashed(Hash128(11))
}

const SEEDED: [(KeyspaceId, u8); 5] = [
	(KeyspaceId::GUEST_ACCUMULATOR, 1),
	(KeyspaceId::GUEST_ACCUMULATOR, 2),
	(KeyspaceId::WINDOW_META, 1),
	(KeyspaceId::EMIT, 1),
	(KeyspaceId::GUEST_ROW_MAPPING, 1),
];

fn inner(keyspace: KeyspaceId, suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(group(), keyspace, [suffix]).into_encoded()
}

fn seeded_state() -> HashMap<EncodedKey, EncodedBytes> {
	SEEDED.iter()
		.map(|(keyspace, suffix)| (inner(*keyspace, *suffix), EncodedBytes(CowVec::new(vec![7u8]))))
		.collect()
}

fn seeded_transaction(state: HashMap<EncodedKey, EncodedBytes>) -> TestFlowTransaction {
	let mut txn = TestFlowTransaction::new(CommitVersion(1), Clock::Mock(MockClock::new(0)));
	for (inner, value) in state {
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(inner.as_slice()).expect("a seeded key decodes");
		txn.pending_mut().insert(OperatorStateKey::encoded(OPERATOR, group, keyspace, suffix), value);
	}
	txn
}

fn expected_in_scan_order(keep: impl Fn(KeyspaceId) -> bool) -> Vec<EncodedKey> {
	let mut keys: Vec<EncodedKey> = SEEDED
		.iter()
		.filter(|(keyspace, _)| keep(*keyspace))
		.map(|(keyspace, suffix)| inner(*keyspace, *suffix))
		.collect();
	keys.sort();
	keys
}

#[test]
fn a_guest_group_sweep_returns_what_a_single_scan_over_the_group_would() {
	// A guest can no longer ask for a whole group in one range, so GuestAsHost sweeps keyspace by
	// keyspace and concatenates. The reaper resumes on the order this returns, so the concatenation
	// must equal the byte order the single group range used to produce. Keyspace bytes are stored
	// complemented, so that order is DESCENDING by keyspace id: a sweep that walked the catalogue in
	// ascending id order would return the same set in the wrong order and only surface as a reaper
	// that reaps the same keys twice and misses others.
	let mut txn = seeded_transaction(seeded_state());
	let mut host = TxnHostContext::new(&mut txn, OPERATOR);
	let mut ctx = InProcessContext::new(&mut host, OPERATOR);

	let swept = GuestAsHost(&mut ctx).group_sweep(group(), KeyspaceMask::all(), None).expect("sweep");
	let swept: Vec<EncodedKey> = swept.into_iter().map(|(key, _)| key.into_encoded()).collect();

	assert_eq!(swept, expected_in_scan_order(|_| true));
}

#[test]
fn a_data_only_guest_group_sweep_leaves_the_identity_keyspaces_alone() {
	// data_only is what separates reaping a group's rows from reclaiming its identity; a sweep that
	// ignored the flag would delete the row number mappings the group is still addressed by.
	let mut txn = seeded_transaction(seeded_state());
	let mut host = TxnHostContext::new(&mut txn, OPERATOR);
	let mut ctx = InProcessContext::new(&mut host, OPERATOR);

	let swept = GuestAsHost(&mut ctx).group_sweep(group(), KeyspaceMask::data(), None).expect("sweep");
	let swept: Vec<EncodedKey> = swept.into_iter().map(|(key, _)| key.into_encoded()).collect();

	assert_eq!(swept, expected_in_scan_order(|keyspace| keyspace.is_data()));
	assert!(
		!swept.contains(&inner(KeyspaceId::GUEST_ROW_MAPPING, 1)),
		"an identity keyspace must survive a data only sweep"
	);
}

#[test]
fn a_guest_group_sweep_spends_one_budget_across_every_keyspace() {
	// The single scan took one limit; the sweep splits it across keyspaces, so the budget has to be
	// decremented as it goes. A per-keyspace limit would return up to limit * keyspaces keys and blow
	// the reaper's budget, and the reaper's "is there more" probe asks for exactly budget + 1.
	let mut txn = seeded_transaction(seeded_state());
	let mut host = TxnHostContext::new(&mut txn, OPERATOR);
	let mut ctx = InProcessContext::new(&mut host, OPERATOR);

	for budget in 0..=SEEDED.len() {
		let mut store = GuestAsHost(&mut ctx);
		let swept = store.group_sweep(group(), KeyspaceMask::all(), Some(budget)).expect("sweep");
		assert_eq!(swept.len(), budget, "a sweep must return exactly the budget it was given");

		let swept: Vec<EncodedKey> = swept.into_iter().map(|(key, _)| key.into_encoded()).collect();
		assert_eq!(swept, expected_in_scan_order(|_| true)[..budget], "and it must take them in order");
	}
}
