// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_core::interface::catalog::{
	id::SubscriptionId,
	subscription::{SubscribeOptions, SubscribeOutcome},
};
use reifydb_engine::subscription::SubscriptionServiceRef;
use reifydb_value::{
	byte_size::ByteSize,
	value::{duration::Duration, identity::IdentityId},
};

fn subscription_id(outcome: SubscribeOutcome) -> SubscriptionId {
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

#[test]
fn dropping_a_subscription_leaves_a_views_operator_state_intact() {
	// A view's operators and a subscription's both number from 1, so an unsubscribe that drops state by operator id
	// alone resets the view's limit.
	let timeout = Duration::from_seconds_const(10);
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t TAKE 2 MAP { id } }");

	db.command("INSERT app::t [{id: 1}, {id: 2}]");
	assert!(db.await_all_flows(timeout), "the view must catch up before the subscription exists");
	assert_eq!(db.row_count("FROM app::v"), 2, "precondition: the view is at its limit");

	let store = db.engine().operator_state();
	let resident = store.total_bytes().unwrap();
	assert!(resident > ByteSize::ZERO, "precondition: the view's operators hold state to lose");

	let outcome = db
		.engine()
		.subscribe_as(IdentityId::root(), "FROM app::t MAP { id }", Params::None, SubscribeOptions::default())
		.expect("subscribe as root");
	let id = subscription_id(outcome);
	let dropped = db
		.engine()
		.ioc()
		.resolve::<SubscriptionServiceRef>()
		.expect("resolve subscription service")
		.unregister_subscription(&id)
		.expect("unsubscribe");
	assert!(dropped, "the subscription was just created, so unsubscribing must find and drop it");

	assert_eq!(
		store.total_bytes().unwrap(),
		resident,
		"no insert ran between the two reads, so any drop here is the unsubscribe taking the view's state"
	);

	db.command("INSERT app::t [{id: 3}]");
	assert!(db.await_all_flows(timeout), "the view must observe the post-unsubscribe insert");

	assert_eq!(
		db.row_count("FROM app::v"),
		2,
		"the view's take state must survive the unsubscribe, otherwise the limit resets and row 3 is admitted"
	);
}
