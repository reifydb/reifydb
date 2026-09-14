// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, testing::db::TestDb};
use reifydb_core::interface::catalog::{
	id::SubscriptionId,
	subscription::{SubscribeOptions, SubscribeOutcome},
};
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;
use reifydb_value::value::identity::IdentityId;

fn listed(db: &TestDb, id: SubscriptionId) -> usize {
	db.row_count(&format!("FROM system::subscriptions FILTER {{ id == {} }}", id.0))
}

#[test]
fn a_failed_worker_registration_leaves_no_subscription_behind() {
	// A failed subscribe returns no id, so any store entry it leaves can never be unregistered by anyone.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::keep { id: int4 }");
	db.admin("CREATE TABLE app::gone { id: int4 }");

	let engine = db.engine();
	let store = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present").store().clone();

	let healthy = match engine
		.subscribe_as(IdentityId::root(), "FROM app::keep", Params::None, SubscribeOptions::default())
		.expect("subscribe to a live table")
	{
		SubscribeOutcome::Local {
			id,
		} => id,
		SubscribeOutcome::Remote {
			address,
			..
		} => panic!("expected a local subscription, got a remote one at {}", address),
	};
	assert_eq!(
		listed(&db, healthy),
		1,
		"precondition: without a listed healthy id the leak check below is vacuous"
	);

	let failed = SubscriptionId(healthy.0 + 1);
	let mut txn = engine.begin_query(IdentityId::root()).expect("begin query");
	db.admin("DROP TABLE app::gone");
	let result = engine.executor().subscribe(&mut txn, "FROM app::gone", Params::None, SubscribeOptions::default());
	drop(txn);

	let err =
		result.err().expect("the worker reads the latest catalog, so the dropped table must fail registration");
	assert!(
		err.0.message.contains("not found in catalog"),
		"precondition: the failure must come from the worker's catalog read, not the snapshot compile: {}",
		err.0.message
	);

	assert!(!store.contains(&failed), "a failed registration must not leave its id in the subscription store");
	assert_eq!(listed(&db, failed), 0, "a failed registration must never be listed in system::subscriptions");
}
