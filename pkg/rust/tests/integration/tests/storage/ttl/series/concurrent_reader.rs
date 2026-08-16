// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{IdentityId, testing::db::TempDbPath};

use crate::storage::ttl::{age_past_ttl, await_evicted, ttl_db};

#[test]
fn eviction_is_not_bound_by_a_held_reader() {
	// Row ttl resolves on wall clock alone, so no reader may ever enter its cutoff.
	let path = TempDbPath::new("ttl_reader_series");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create series test::s { ts: int8, n: int4 } with { time: processing, key: ts, row: { ttl: 1s } }");
	db.command("insert test::s [{ ts: 1, n: 1 }, { ts: 2, n: 2 }, { ts: 3, n: 3 }]");

	let reader = db.engine().begin_query(IdentityId::root()).unwrap();
	let (_version, lease) = db.engine().acquire_current_snapshot_lease().unwrap();
	assert!(db.engine().multi().leases().min_active().is_some(), "the lease must be active or this proves nothing");

	age_past_ttl();
	await_evicted(&db, "from test::s", 0);

	drop(lease);
	drop(reader);
	assert_eq!(db.row_count("from test::s"), 0, "releasing the reader must not resurrect evicted rows");
	db.stop();
}
