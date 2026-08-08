// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A frontier published inside the sweep interval reaches disk because graceful shutdown sweeps once
//! in the quiet window between the CDC drain and the single-store flush. The registry itself has no
//! running-flow observable - a hydrated entry resolves as withheld until its producer republishes - so
//! this suite reads the persisted keys directly rather than through the query surface.

use std::time::Duration;

use reifydb::{
	SqliteConfig, WithSubsystem,
	core::{common::CommitVersion, interface::store::SingleVersionRange, key::output_frontier::OutputFrontierKey},
	embedded,
	testing::db::{TempDbPath, TestDb},
};

const TIMEOUT: Duration = Duration::from_secs(10);
const SCAN_BATCH: u64 = 1024;

fn open(path: &TempDbPath) -> TestDb {
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			.with_flow(|f| f)
			.build()
			.expect("build a sqlite database with the flow subsystem"),
	)
}

fn declare(db: &TestDb) {
	db.admin("CREATE NAMESPACE fsd");
	db.admin("CREATE TABLE fsd::src { id: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE DEFERRED VIEW fsd::out { id: int4, v: int4, ts: datetime } AS { FROM fsd::src }");
}

/// The stamp is the trailing eight big-endian bytes of the frontier value; only the version is read
/// here because the frontier instant says nothing about which sweep wrote it.
fn persisted_stamps(db: &TestDb) -> Vec<CommitVersion> {
	let store = db.engine().single().read_store();
	let batch = SingleVersionRange::range_batch(&store, OutputFrontierKey::full_scan(), SCAN_BATCH)
		.expect("scan the output frontier keyspace");
	batch.items
		.iter()
		.filter_map(|row| {
			let bytes = row.row.as_slice();
			(bytes.len() == 16).then(|| CommitVersion(u64::from_be_bytes(bytes[8..].try_into().unwrap())))
		})
		.collect()
}

#[test]
fn a_frontier_published_after_the_last_sweep_survives_a_graceful_shutdown() {
	// Every earlier sweep stamped at or below `before`, so a stamp above it can only have been written on the way
	// out.
	let path = TempDbPath::new("frontier_shutdown");

	let before;
	{
		let mut db = open(&path);
		declare(&db);

		db.command(r#"INSERT fsd::src [{ id: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
		db.await_row_count("FROM fsd::out", 1, TIMEOUT);

		before = db.engine().current_version().expect("read the current commit version");

		db.command(r#"INSERT fsd::src [{ id: 2, v: 7, ts: "2026-01-01T00:00:10Z" }]"#);
		assert!(db.await_all_flows(TIMEOUT), "the final row must be published before the shutdown is judged");

		db.stop();
	}

	let mut db = open(&path);
	let stamps = persisted_stamps(&db);

	assert!(
		stamps.iter().any(|at| *at > before),
		"a frontier stamped above {:?} must be on disk after a graceful shutdown; found {:?}",
		before,
		stamps
	);

	db.stop();
}
