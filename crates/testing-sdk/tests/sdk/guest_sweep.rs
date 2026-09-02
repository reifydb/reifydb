// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::{GroupId, KeyspaceId, OperatorStateKey},
	state::timer::StateStore,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		change::BorrowedChange,
		column::operator::OperatorColumn,
		extern_c::binding::{context::ExternCContext, operator::ExternCOperator},
		windowed::guest_as_host::GuestAsHost,
	},
};
use reifydb_testing_sdk::harness::ExternCOperatorHarnessBuilder;
use reifydb_value::{config::Config, util::cowvec::CowVec};

struct SweepOp;

impl OperatorMetadata for SweepOp {
	const NAME: &'static str = "guest_sweep_op";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ExternCOperator for SweepOp {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, _: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
		Ok(())
	}
}

const GROUP: GroupId = GroupId(11);

const SEEDED: [(KeyspaceId, u8); 5] = [
	(KeyspaceId::JOIN_LEFT, 1),
	(KeyspaceId::JOIN_LEFT, 2),
	(KeyspaceId::WINDOW_META, 1),
	(KeyspaceId::ACCUMULATOR, 1),
	(KeyspaceId::GUEST_ROW_MAPPING, 1),
];

fn inner(keyspace: KeyspaceId, suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, keyspace, [suffix]).into_encoded()
}

fn seeded_state() -> HashMap<EncodedKey, EncodedBytes> {
	SEEDED.iter()
		.map(|(keyspace, suffix)| (inner(*keyspace, *suffix), EncodedBytes(CowVec::new(vec![7u8]))))
		.collect()
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
	let mut harness = ExternCOperatorHarnessBuilder::<SweepOp>::new().build().expect("harness");
	harness.restore_state(seeded_state());
	let mut ctx = harness.create_operator_context();

	let swept = GuestAsHost(&mut ctx).group_sweep(GROUP, false, None).expect("sweep");
	let swept: Vec<EncodedKey> = swept.into_iter().map(|(key, _)| key.into_encoded()).collect();

	assert_eq!(swept, expected_in_scan_order(|_| true));
}

#[test]
fn a_data_only_guest_group_sweep_leaves_the_identity_keyspaces_alone() {
	// data_only is what separates reaping a group's rows from reclaiming its identity; a sweep that
	// ignored the flag would delete the row number mappings the group is still addressed by.
	let mut harness = ExternCOperatorHarnessBuilder::<SweepOp>::new().build().expect("harness");
	harness.restore_state(seeded_state());
	let mut ctx = harness.create_operator_context();

	let swept = GuestAsHost(&mut ctx).group_sweep(GROUP, true, None).expect("sweep");
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
	let mut harness = ExternCOperatorHarnessBuilder::<SweepOp>::new().build().expect("harness");
	harness.restore_state(seeded_state());
	let mut ctx = harness.create_operator_context();

	for budget in 0..=SEEDED.len() {
		let mut store = GuestAsHost(&mut ctx);
		let swept = store.group_sweep(GROUP, false, Some(budget)).expect("sweep");
		assert_eq!(swept.len(), budget, "a sweep must return exactly the budget it was given");

		let swept: Vec<EncodedKey> = swept.into_iter().map(|(key, _)| key.into_encoded()).collect();
		assert_eq!(swept, expected_in_scan_order(|_| true)[..budget], "and it must take them in order");
	}
}
