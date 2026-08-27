// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;

use crate::common::{
	Row, drain_sub, extract_sub_id, insert_one_at_a_time, make_db, normalize, random_rows, run_path_snapshot,
	wait_for_consumer_caught_up,
};

// Every other fixture in this suite tops out at nine rows, so none of them ever crosses the 1024-batch capacity.

const OVER_RING: usize = 2000;

fn overrun(db: &TestDb, sub_id: SubscriptionId) -> Option<u16> {
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	subsystem.store().overrun(&sub_id)
}

#[test]
fn hydration_delivers_every_row_of_a_fixture_larger_than_the_ring() {
	// The snapshot bypasses the delivery buffer, so buffer capacity must not bound how much a subscriber hydrates.
	let rows: Vec<Row> = random_rows(7, OVER_RING, 1000);

	let delivered = normalize(run_path_snapshot("from app::t", &rows));

	assert_eq!(delivered.len(), OVER_RING, "hydration must deliver every seeded row, not a buffer's worth");
}

#[test]
fn a_live_subscriber_that_falls_behind_is_terminated_not_truncated() {
	// Truncating leaves the subscriber holding removes for rows it never received, so falling behind must end the
	// subscription.
	let rows: Vec<Row> = random_rows(7, OVER_RING, 1000);
	let db = make_db();

	let frames = db.admin("CREATE SUBSCRIPTION AS { from app::t }");
	let sub_id = extract_sub_id(&frames);

	insert_one_at_a_time(&db, &rows);

	wait_for_consumer_caught_up(&db);

	let overran = overrun(&db, sub_id).expect("a subscriber that never drains past capacity must be marked lagged");
	assert!(overran > 0, "the recorded overrun must say how far past capacity the subscriber fell");
	assert!(
		drain_sub(&db, sub_id).is_empty(),
		"a lagged subscription must deliver no partial state, since a prefix is indistinguishable from the whole"
	);
}
