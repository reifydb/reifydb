// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, testing::db::TestDb};
use reifydb_core::interface::{
	catalog::{
		id::SubscriptionId,
		subscription::{HydrationConfig, SubscribeOptions, SubscribeOutcome},
	},
	change::StagedBatch,
};
use reifydb_engine::subscription::SubscriptionServiceRef;
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;
use reifydb_value::value::{Value, duration::Duration, identity::IdentityId, system_columns::column_view};

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

fn drain(db: &TestDb, sub_id: SubscriptionId) -> Vec<i32> {
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	let mut out = Vec::new();
	for (_, batch) in subsystem.store().drain(&sub_id, usize::MAX) {
		let id_col = column_view(&batch, "id").unwrap().expect("id column");
		for i in 0..batch.num_rows() {
			match id_col.get_value(i) {
				Value::Int4(v) => out.push(v),
				other => panic!("expected Int4 id, got {:?}", other),
			}
		}
	}
	out
}

fn snapshot_ids(batches: &[StagedBatch]) -> Vec<i32> {
	let mut out = Vec::new();
	for (_, batch) in batches {
		let id_col = column_view(batch, "id").unwrap().expect("id column");
		for i in 0..batch.num_rows() {
			match id_col.get_value(i) {
				Value::Int4(v) => out.push(v),
				other => panic!("expected Int4 id, got {:?}", other),
			}
		}
	}
	out.sort();
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
fn a_subscription_without_hydration_delivers_only_rows_after_registration() {
	// A floor below the seeding version lets the pre-existing row reach the store once the consumer catches up.
	let db = make_db();
	db.command("INSERT app::t [{id: 1}]");

	let sub_id = extract_sub_id(subscribe(&db, false));

	wait_for_consumer_caught_up(&db);

	assert_eq!(
		drain(&db, sub_id),
		Vec::<i32>::new(),
		"a row committed before registration must never be delivered"
	);
}

#[test]
fn a_subscription_with_hydration_delivers_every_row_once_across_the_hand_off() {
	// A row at or below the snapshot must arrive only in it and a later row only live, or one is lost or doubled.
	let db = make_db();
	db.command("INSERT app::t [{id: 1}]");

	let sub_id = extract_sub_id(subscribe(&db, true));

	db.command("INSERT app::t [{id: 2}]");
	let engine = db.engine().clone();
	let (_, lease) = engine.acquire_current_snapshot_lease().expect("acquire lease");
	let sub_service = engine.services().ioc.resolve::<SubscriptionServiceRef>().expect("resolve service");
	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 1024).expect("hydrate succeeds");

	db.command("INSERT app::t [{id: 3}]");
	wait_for_consumer_caught_up(&db);

	assert_eq!(
		snapshot_ids(&outcome.batches),
		vec![1, 2],
		"the snapshot must hold every row committed up to its version"
	);
	assert_eq!(drain(&db, sub_id), vec![3], "only the row committed after the snapshot may arrive live");
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
