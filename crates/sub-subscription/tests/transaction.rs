// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{QueueId, TableId},
		storage::StorageId,
	},
	key::{queue::QueueDeduplicationKey, row::RowKey},
};
use reifydb_flow_async::transaction::{
	ChangeCoordinate, FlowTransaction, state::StateExtension, substrate::FlowSubstrate,
};
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
		FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state()),
	);
	txn.set_change_coordinate(ChangeCoordinate {
		at: Some(DateTime::from_millis(0)),
	});
	txn
}

fn make_value(s: &str) -> EncodedPodRow {
	EncodedPodRow::new(s.as_bytes())
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
	let row = RowKey::new(StorageId::table(TableId(7)), RowNumber(1));
	let row_key = row.encode();
	let row_value = make_value("own_row").into_bytes();

	let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
	cmd.disable_conflict_tracking().unwrap();
	cmd.set(
		&QueueDeduplicationKey::new(QueueId(1), b"warmup".iter().map(|b| !b).collect::<Vec<u8>>()),
		make_value("w").into_bytes(),
	)
	.unwrap();
	let low_version = cmd.commit_unchecked().unwrap();

	let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
	cmd.disable_conflict_tracking().unwrap();
	cmd.set(&row, row_value).unwrap();
	let committed_at = cmd.commit_unchecked().unwrap();
	assert!(low_version < committed_at);

	let mut txn = EphemeralTransaction::new(
		low_version,
		engine.multi().begin_query().unwrap(),
		Catalog::testing(),
		HashMap::new(),
		engine.clock().clone(),
		FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state()),
	);
	assert_eq!(
		txn.get(&row_key).unwrap(),
		None,
		"ephemeral (subscription) row reads must stay pinned to the requested version"
	);
}
