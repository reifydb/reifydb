// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread;

use reifydb::{Params, testing::db::TestDb};
use reifydb_core::interface::catalog::{
	id::SubscriptionId,
	subscription::{HydrationConfig, SubscribeOptions, SubscribeOutcome},
};
use reifydb_engine::subscription::SubscriptionServiceRef;
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;
use reifydb_value::value::{
	Value,
	duration::Duration,
	identity::IdentityId,
	system_columns::{column_view, row_numbers},
};

fn subscribe(db: &TestDb, rql: &str, options: SubscribeOptions) -> SubscriptionId {
	match db.engine().subscribe_as(IdentityId::root(), rql, Params::None, options).expect("subscribe as root") {
		SubscribeOutcome::Local {
			id,
		} => id,
		SubscribeOutcome::Remote {
			address,
			..
		} => panic!("expected a local subscription, got a remote one at {}", address),
	}
}

fn table(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, qty: int4 }");
}

#[test]
fn a_live_subscription_carries_rownum_it_never_names() {
	// Clients key subscription rows by #rownum, so the live feed must carry it although the query never names it.
	let db = TestDb::memory();
	table(&db);
	let options = SubscribeOptions {
		hydration: HydrationConfig {
			enabled: false,
			max_rows: None,
		},
		..SubscribeOptions::default()
	};
	let sub_id = subscribe(&db, "from app::t map { id }", options);

	db.command("INSERT app::t [{id: 10, qty: 1}, {id: 20, qty: 2}]");
	let target = db.watermarks().tx().current().expect("current version");
	assert!(
		db.watermarks().cdc().wait_for_consumer(target, Duration::from_seconds(10).unwrap()),
		"the subscription consumer must finish the insert"
	);

	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	let mut seen: Vec<(i32, u64)> = Vec::new();
	for (_, batch) in subsystem.store().drain(&sub_id, usize::MAX) {
		assert_eq!(
			row_numbers(&batch).unwrap().len(),
			batch.num_rows(),
			"every delivered row must carry its #rownum"
		);
		let id_col = column_view(&batch, "id").unwrap().expect("id column");
		for (i, row_number) in row_numbers(&batch).unwrap().iter().enumerate() {
			match id_col.get_value(i) {
				Value::Int4(id) => seen.push((id, row_number.value())),
				other => panic!("expected Int4 id, got {:?}", other),
			}
		}
	}
	seen.sort();
	assert_eq!(seen, vec![(10, 1), (20, 2)], "each row must carry its own stored row number");
}

#[test]
fn hydration_carries_rownum_it_never_names() {
	// Hydration answers through a plain query, which must still carry the row numbers the feed keys on.
	let db = TestDb::memory();
	table(&db);
	db.command("INSERT app::t [{id: 10, qty: 1}, {id: 20, qty: 2}]");

	let sub_id = subscribe(&db, "from app::t map { id }", SubscribeOptions::default());
	let engine = db.engine().clone();
	let (_, lease) = engine.acquire_current_snapshot_lease().expect("acquire lease");
	let sub_service = engine.services().ioc.resolve::<SubscriptionServiceRef>().expect("resolve service");
	thread::sleep(Duration::from_milliseconds(50).unwrap().to_std());

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 1024).expect("hydrate succeeds");

	let mut seen: Vec<u64> = Vec::new();
	for (_, batch) in &outcome.batches {
		assert_eq!(
			row_numbers(batch).unwrap().len(),
			batch.num_rows(),
			"every hydrated row must carry its #rownum"
		);
		seen.extend(row_numbers(batch).unwrap().iter().map(|row| row.value()));
	}
	seen.sort();
	assert_eq!(seen, vec![1, 2], "the snapshot must carry the stored row numbers");
}
