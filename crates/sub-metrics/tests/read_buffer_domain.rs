// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read-buffer metrics domain: catalog surface and vtable registration, driven through the wired subsystem.
//!
//! A bare in-memory database has no read tier and therefore no read buffer, so each of the three
//! `read_buffer::*::current` surfaces the subsystem registers must be queryable by its RQL path and empty rather
//! than error, and each `::snapshots` series must exist from bootstrap and stay empty because nothing writes it
//! yet. The column specs are pinned to the approved schema widths so the vtable and the snapshots series cannot
//! drift apart silently.

use reifydb_sub_metrics::domains::read_buffer::ReadBufferDomain;
use reifydb_test_harness::db::TestDb;

#[test]
fn read_buffer_current_and_snapshots_are_queryable_after_bootstrap() {
	let db = TestDb::memory();

	for table in ["shards", "warms", "reads"] {
		assert_eq!(
			db.row_count(&format!("from system::metrics::read_buffer::{table}::current")),
			0,
			"{table}::current must be queryable and empty for a store without a read tier",
		);

		assert_eq!(
			db.row_count(&format!("from system::metrics::read_buffer::{table}::snapshots")),
			0,
			"{table}::snapshots must exist from bootstrap and stay unpopulated",
		);
	}
}

#[test]
fn read_buffer_column_specs_match_the_snapshot_schemas() {
	// The snapshots series widths are fixed at compile time by the
	// ColumnId arrays (13/10/8); the current vtables must declare the
	// same shape or the two surfaces of one domain would disagree.
	let widths = [(ReadBufferDomain::Shards, 13), (ReadBufferDomain::Warms, 10), (ReadBufferDomain::Reads, 8)];
	for (domain, expected) in widths {
		let columns = domain.columns();
		assert_eq!(columns.len(), expected, "{domain:?} column count");
		assert_eq!(columns[0].name, "ts");
		assert_eq!(columns[1].name, "domain");
		assert_eq!(columns[2].name, "shard");
	}
}
