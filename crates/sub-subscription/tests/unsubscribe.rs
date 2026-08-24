// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_value::{
	byte_size::ByteSize,
	value::{Value, frame::frame::Frame},
};

fn subscription_name(frames: &[Frame]) -> String {
	let frame = frames.first().expect("subscription frame");
	let value = frame
		.columns
		.iter()
		.find(|c| c.name == "subscription_id")
		.map(|c| c.data.get_value(0))
		.expect("subscription_id column");
	match value {
		Value::Uint8(n) => format!("subscription_{}", n),
		other => panic!("unexpected subscription_id value: {:?}", other),
	}
}

#[test]
fn dropping_a_subscription_leaves_a_views_operator_state_intact() {
	// A view's operators and a subscription's both number from 1, so an unsubscribe that drops state by operator id
	// alone resets the view's limit.
	let timeout = Duration::from_secs(10);
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t TAKE 2 MAP { id } }");

	db.command("INSERT app::t [{id: 1}, {id: 2}]");
	assert!(db.await_all_flows(timeout), "the view must catch up before the subscription exists");
	assert_eq!(db.row_count("FROM app::v"), 2, "precondition: the view is at its limit");

	let store = db.engine().operator_state();
	let resident = store.total_bytes();
	assert!(resident > ByteSize::ZERO, "precondition: the view's operators hold state to lose");

	let frames = db.admin("CREATE SUBSCRIPTION AS { FROM app::t MAP { id } }");
	let name = subscription_name(&frames);
	db.admin(&format!("DROP SUBSCRIPTION {}", name));

	assert_eq!(
		store.total_bytes(),
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
