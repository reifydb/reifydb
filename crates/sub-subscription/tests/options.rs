// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, testing::db::TestDb};
use reifydb_core::interface::catalog::{
	id::SubscriptionId,
	subscription::{HydrationConfig, SubscribeOptions, SubscribeOutcome},
};
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;
use reifydb_value::{
	Result,
	error::Diagnostic,
	value::{duration::Duration, identity::IdentityId},
};

fn make_db() -> TestDb {
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db
}

fn subscribe(db: &TestDb, query: &str, options: SubscribeOptions) -> Result<SubscribeOutcome> {
	db.engine().subscribe_as(IdentityId::root(), query, Params::None, options)
}

fn rejection(db: &TestDb, query: &str, options: SubscribeOptions) -> Diagnostic {
	subscribe(db, query, options).err().expect("expected the subscription to be rejected").diagnostic()
}

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

fn active_subscriptions(db: &TestDb) -> Vec<SubscriptionId> {
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	subsystem.store().active_subscriptions()
}

fn max_rows(max_rows: Option<u64>) -> SubscribeOptions {
	SubscribeOptions {
		hydration: HydrationConfig {
			enabled: true,
			max_rows,
		},
		..SubscribeOptions::default()
	}
}

#[test]
fn a_max_rows_of_zero_is_rejected_and_registers_no_subscription() {
	// A zero cap hydrates nothing while looking like a success, so it must be refused before anything registers.
	let db = make_db();

	let diag = rejection(&db, "from app::t", max_rows(Some(0)));

	assert_eq!(diag.code, "SUBS_007", "expected SUBS_007, got {:?}: {}", diag.code, diag.message);
	assert!(active_subscriptions(&db).is_empty(), "a rejected subscribe must not leave a subscription registered");
}

#[test]
fn a_negative_throttle_is_rejected() {
	// A negative throttle has no meaning as a delivery interval, so it must never reach a live subscription.
	let db = make_db();
	let options = SubscribeOptions {
		throttle: Some(Duration::from_milliseconds(-1).unwrap()),
		..SubscribeOptions::default()
	};

	let diag = rejection(&db, "from app::t", options);

	assert_eq!(diag.code, "SUBS_008", "expected SUBS_008, got {:?}: {}", diag.code, diag.message);
	assert!(active_subscriptions(&db).is_empty(), "a rejected subscribe must not leave a subscription registered");
}

#[test]
fn a_negative_linger_is_rejected() {
	// A negative linger has no meaning as a wait, so it must never reach a live subscription.
	let db = make_db();
	let options = SubscribeOptions {
		linger: Some(Duration::from_milliseconds(-1).unwrap()),
		..SubscribeOptions::default()
	};

	let diag = rejection(&db, "from app::t", options);

	assert_eq!(diag.code, "SUBS_009", "expected SUBS_009, got {:?}: {}", diag.code, diag.message);
	assert!(active_subscriptions(&db).is_empty(), "a rejected subscribe must not leave a subscription registered");
}

#[test]
fn a_zero_throttle_and_a_zero_linger_are_accepted() {
	// Zero is the non-negative boundary, so a check that also refuses zero breaks unthrottled delivery.
	let db = make_db();
	let options = SubscribeOptions {
		throttle: Some(Duration::zero()),
		linger: Some(Duration::zero()),
		..SubscribeOptions::default()
	};

	let id = extract_sub_id(
		subscribe(&db, "from app::t", options).expect("zero throttle and linger must be accepted"),
	);

	assert!(active_subscriptions(&db).contains(&id), "an accepted subscribe must register its subscription");
}

#[test]
fn a_max_rows_of_one_is_accepted() {
	// One is the smallest valid cap, so a check written as max_rows <= 1 would refuse it.
	let db = make_db();

	let id = extract_sub_id(
		subscribe(&db, "from app::t", max_rows(Some(1))).expect("max_rows of one must be accepted"),
	);

	assert!(active_subscriptions(&db).contains(&id), "an accepted subscribe must register its subscription");
}

#[test]
fn a_statement_that_is_not_a_query_is_rejected() {
	// A subscribe that accepts a DML statement turns the subscription endpoint into a write path.
	let db = make_db();

	let diag = rejection(&db, "INSERT app::t [{id: 1}]", SubscribeOptions::default());

	assert_eq!(diag.code, "SUBS_010", "expected SUBS_010, got {:?}: {}", diag.code, diag.message);
	assert!(active_subscriptions(&db).is_empty(), "a rejected subscribe must not leave a subscription registered");
	assert_eq!(db.row_count("from app::t"), 0, "a rejected subscribe must not have executed the INSERT");
}
