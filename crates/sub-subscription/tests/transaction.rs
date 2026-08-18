// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{flow::OperatorId, id::TableId, storage::StorageId},
	key::{
		operator_state::{GroupStateKey, OperatorStateKey},
		row::RowKey,
	},
};
use reifydb_flow::transaction::{ChangeCoordinate, FlowTransaction, state::StateExtension};
use reifydb_sub_subscription::transaction::EphemeralTransaction;
use reifydb_test_harness::{
	engine::TestEngine,
	operator::transaction::{OPERATOR_ID, engine, key, make_row},
};
use reifydb_value::value::{datetime::DateTime, identity::IdentityId, row_number::RowNumber};

fn ephemeral(engine: &TestEngine) -> EphemeralTransaction {
	// The coordinate is fixed so a stamp never falls back to the wall clock.
	let version = CommitVersion(1);
	let mut txn = EphemeralTransaction::new(
		version,
		engine.multi().begin_query().unwrap(),
		Catalog::testing(),
		HashMap::new(),
		engine.clock().clone(),
	);
	txn.set_change_coordinate(ChangeCoordinate {
		at: Some(DateTime::from_millis(0)),
		version,
	});
	txn
}

fn make_value(s: &str) -> EncodedPodRow {
	EncodedPodRow::new(s.as_bytes())
}

fn full_key(operator: OperatorId, key: &GroupStateKey) -> EncodedKey {
	let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
		.expect("scoped state keys must carry a structured inner encoding");
	OperatorStateKey::encoded(operator, group, keyspace, suffix)
}

#[test]
fn update_replaces_the_row_wholesale() {
	// A repeat state_set must replace the stored row wholesale, or a merging write would leave the earlier body
	// readable.
	let e = engine();
	let mut txn = ephemeral(&e);
	let k = key("update-key");

	txn.state_set(OPERATOR_ID, &k, make_row("v1")).unwrap();
	txn.state_set(OPERATOR_ID, &k, make_row("v2")).unwrap();

	let stored = txn.state_get(OPERATOR_ID, &k).unwrap().unwrap();
	assert_eq!(stored.body(), b"v2");
}

#[test]
fn row_reads_stay_pinned_to_requested_version() {
	// Hydration reads as-of a version, so a row committed above it must never become visible.
	let engine = TestEngine::new();
	let row_key = RowKey::encoded(StorageId::table(TableId(7)), RowNumber(1));
	let row_value = make_value("own_row").into_bytes();

	let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
	cmd.disable_conflict_tracking().unwrap();
	cmd.set(&key("warmup").into_encoded(), make_value("w").into_bytes()).unwrap();
	let low_version = cmd.commit_unchecked().unwrap();

	let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
	cmd.disable_conflict_tracking().unwrap();
	cmd.set(&row_key, row_value).unwrap();
	let committed_at = cmd.commit_unchecked().unwrap();
	assert!(low_version < committed_at);

	let mut txn = EphemeralTransaction::new(
		low_version,
		engine.multi().begin_query().unwrap(),
		Catalog::testing(),
		HashMap::new(),
		engine.clock().clone(),
	);
	assert_eq!(
		txn.get(&row_key).unwrap(),
		None,
		"ephemeral (subscription) row reads must stay pinned to the requested version"
	);
}

#[test]
fn read_sees_state_map_and_pending() {
	// With no state_query, operator-state reads must come from the state map under the pending overlay.
	let engine = TestEngine::new();
	let operator_id = OperatorId(1);
	let seeded_key = key("seeded");
	let seeded_value = make_value("seeded_value");

	let mut state = HashMap::new();
	state.insert(full_key(operator_id, &seeded_key), seeded_value.clone().into_bytes());

	let mut txn = EphemeralTransaction::new(
		CommitVersion(1),
		engine.multi().begin_query().unwrap(),
		Catalog::testing(),
		state,
		engine.clock().clone(),
	);

	let seeded = txn.state_get_many(operator_id, &[seeded_key]).unwrap();
	assert_eq!(seeded.items.len(), 1, "seeded ephemeral state must be readable");
	assert_eq!(seeded.items[0].bytes, seeded_value.into_bytes());

	let live_key = key("live");
	let live_value = make_value("live_value");
	txn.state_set(operator_id, &live_key, live_value.clone()).unwrap();
	let live = txn.state_get_many(operator_id, &[live_key]).unwrap();
	assert_eq!(live.items.len(), 1);
	assert_eq!(live.items[0].bytes, live_value.into_bytes());
}
