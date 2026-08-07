// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// #time is stamped once, at the source, and every downstream view inherits it verbatim. Nothing
// between the source and the sink may re-stamp it.
//
// This is the invariant the production freeze violated: a flow re-stamped #time to arrival on
// entry, so rows carrying July event timestamps reached the windowed operators claiming to be from
// August. The window coordinate and the seal horizon then disagreed about which clock they were on
// and every bucket was discarded. A declaration check could not see that; only reading the value
// back at the end of the chain can.

use std::time::Duration as StdDuration;

use reifydb::testing::db::TestDb;
use reifydb::{WithSubsystem, embedded};

const TIMEOUT: StdDuration = StdDuration::from_secs(5);
const BLOCK_TIME: &str = "2020-01-01T00:00:00Z";

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

fn only_time(db: &TestDb, rql: &str) -> reifydb_value::value::datetime::DateTime {
	let frames = db.query(rql);
	let time = frames[0].time();
	assert_eq!(time.len(), 1, "expected exactly one row from `{rql}`");
	time[0]
}

#[test]
fn an_event_time_source_propagates_its_stamp_through_a_chain_of_views() {
	// Two views deep the row must still report when it HAPPENED, not when it was ingested. If any
	// hop re-stamps, this reads back as "now" and every windowed rollup downstream silently
	// buckets by wall clock while looking perfectly healthy.
	let db = setup();
	db.admin("CREATE NAMESPACE cv");
	db.admin("CREATE TABLE cv::src { id: int4, at: datetime } with { time: event(at) }");
	db.admin("CREATE DEFERRED VIEW cv::upstream { id: int4, at: datetime } AS { FROM cv::src }");
	db.admin("CREATE DEFERRED VIEW cv::downstream { id: int4, at: datetime } AS { FROM cv::upstream }");

	db.command(&format!(r#"INSERT cv::src [{{ id: 1, at: "{BLOCK_TIME}" }}]"#));
	db.await_row_count("FROM cv::downstream", 1, TIMEOUT);

	let source = only_time(&db, "FROM cv::src");
	assert_eq!(only_time(&db, "FROM cv::upstream"), source, "one view deep must carry the source's own stamp");
	assert_eq!(
		only_time(&db, "FROM cv::downstream"),
		source,
		"two views deep too; a re-stamp anywhere in the chain shows up here"
	);
}

#[test]
fn a_processing_time_source_propagates_its_arrival_stamp_unchanged() {
	// Processing time stays the default and is perfectly legal. What matters is that its rows
	// carry ONE time all the way down rather than being re-stamped per hop, so a window
	// coordinate and a watermark taken from the same row can never disagree.
	let db = setup();
	db.admin("CREATE NAMESPACE cv");
	db.admin("CREATE TABLE cv::src { id: int4, at: datetime } with { time: processing }");
	db.admin("CREATE DEFERRED VIEW cv::upstream { id: int4, at: datetime } AS { FROM cv::src }");
	db.admin("CREATE DEFERRED VIEW cv::downstream { id: int4, at: datetime } AS { FROM cv::upstream }");

	db.command(&format!(r#"INSERT cv::src [{{ id: 1, at: "{BLOCK_TIME}" }}]"#));
	db.await_row_count("FROM cv::downstream", 1, TIMEOUT);

	let arrival = only_time(&db, "FROM cv::src");
	assert_ne!(
		arrival,
		reifydb_value::value::datetime::DateTime::from_ymd_hms(2020, 1, 1, 0, 0, 0).unwrap(),
		"precondition: an undeclared source ignores the data column and stamps arrival"
	);
	assert_eq!(only_time(&db, "FROM cv::upstream"), arrival, "one view deep keeps the arrival stamp");
	assert_eq!(only_time(&db, "FROM cv::downstream"), arrival, "two views deep keeps the same stamp");
}
