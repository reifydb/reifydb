// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, testing::db::TestDb};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::SubscriptionId,
		subscription::{HydrationConfig, SubscribeOptions, SubscribeOutcome},
	},
};
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;
use reifydb_value::value::{Value, duration::Duration, identity::IdentityId};

fn extract_sub_id(outcome: SubscribeOutcome) -> SubscriptionId {
	match outcome {
		SubscribeOutcome::Local {
			id,
		} => id,
		SubscribeOutcome::Remote {
			address,
			..
		} => panic!("expected a local subscription, got a remote one at {}", address),
	}
}

fn subscribe(db: &TestDb, hydration_enabled: bool) -> SubscribeOutcome {
	let options = SubscribeOptions {
		hydration: HydrationConfig {
			enabled: hydration_enabled,
			max_rows: None,
		},
		..SubscribeOptions::default()
	};
	db.engine().subscribe_as(IdentityId::root(), "from app::t", Params::None, options).expect("subscribe as root")
}

fn make_db() -> TestDb {
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db
}

fn gate(db: &TestDb, sub_id: SubscriptionId) -> Option<CommitVersion> {
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	subsystem.gate(&sub_id)
}

fn drain(db: &TestDb, sub_id: SubscriptionId) -> Vec<i32> {
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	let mut out = Vec::new();
	for (_, batch) in subsystem.store().drain(&sub_id, usize::MAX) {
		let id_col = batch.iter().find(|c| c.name().text() == "id").expect("id column");
		for i in 0..batch.row_count() {
			match id_col.data().get_value(i) {
				Value::Int4(v) => out.push(v),
				other => panic!("expected Int4 id, got {:?}", other),
			}
		}
	}
	out
}

fn wait_for_consumer_caught_up(db: &TestDb) {
	let target = db.watermarks().tx().current().expect("current version");
	let timeout = Duration::from_seconds(10).unwrap();
	if !db.watermarks().cdc().wait_for_consumer(target, timeout) {
		panic!(
			"CDC consumer did not reach {:?} within {:?} (current consumer = {:?})",
			target,
			timeout,
			db.watermarks().cdc().consumer()
		);
	}
}

#[test]
fn a_subscription_without_hydration_is_gated_at_its_registration_version() {
	// Without a floor the pre-existing row is delivered whenever the consumer reaches that version after
	// registration.
	let db = make_db();
	db.command("INSERT app::t [{id: 1}]");
	let seeded_at = db.watermarks().tx().current().expect("current version");

	let sub_id = extract_sub_id(subscribe(&db, false));

	let gate = gate(&db, sub_id).expect("a subscription with hydration disabled must still carry a version floor");
	assert!(
		gate >= seeded_at,
		"the floor must sit at or above the version that seeded the table, otherwise the pre-existing row is \
		 still deliverable (gate={:?} seeded_at={:?})",
		gate,
		seeded_at
	);
}

#[test]
fn a_subscription_with_hydration_is_gated_at_its_registration_version() {
	// Both paths must pin the same floor, or the snapshot and the live stream meet with a gap or an overlap.
	let db = make_db();
	db.command("INSERT app::t [{id: 1}]");
	let seeded_at = db.watermarks().tx().current().expect("current version");

	let sub_id = extract_sub_id(subscribe(&db, true));

	let gate = gate(&db, sub_id).expect("a hydrating subscription must carry a version floor");
	assert!(gate >= seeded_at, "gate={:?} seeded_at={:?}", gate, seeded_at);
}

#[test]
fn the_gate_admits_changes_committed_after_registration_and_refuses_the_ones_before() {
	// A floor set too high swallows the live stream, so the delivered set must be exact and not merely non-empty.
	let db = make_db();
	db.command("INSERT app::t [{id: 1}]");

	let sub_id = extract_sub_id(subscribe(&db, false));

	db.command("INSERT app::t [{id: 2}]");
	wait_for_consumer_caught_up(&db);

	assert_eq!(drain(&db, sub_id), vec![2], "only the row committed after registration may be delivered");
}
