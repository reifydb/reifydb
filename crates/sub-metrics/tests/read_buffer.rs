// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read-buffer metrics: catalog surface and vtable registration, driven through the wired subsystem.
//!
//! A bare in-memory database has no read tier, so the merged `read_buffer::current` and `read_buffer::total`
//! must be queryable by their RQL paths and empty rather than error. The column layout is pinned against the
//! DomainSpec so the published surface cannot drift from the declared one silently.

use reifydb::testing::db::TestDb;
use reifydb_core::metrics::sample::MetricKind;
use reifydb_sub_metrics::framework::spec::{MetricsDomain, Surface};

#[test]
fn read_buffer_current_and_total_are_queryable_after_bootstrap() {
	let db = TestDb::memory();

	for table in ["current", "total"] {
		assert_eq!(
			db.row_count(&format!("from system::metrics::read_buffer::{table}")),
			0,
			"read_buffer::{table} must be queryable and empty for a store without a read tier",
		);
	}
}

#[test]
fn read_buffer_spec_pins_the_merged_layout() {
	// One merged table replaced shards/warms/reads: the namespace was only serving as the
	// kind marker, so the split line (levels vs counters) must now live in the kind, not in
	// a table name, and no row may carry a domain discriminator column.
	let spec = MetricsDomain::ReadBuffer.spec();

	let current = spec.columns(Surface::Current);
	assert_eq!(current.len(), 24, "ts + shard + 10 levels + 12 counters");
	assert_eq!(current[0].name, "ts");
	assert_eq!(current[1].name, "shard");
	assert!(current.iter().all(|column| column.name != "domain"), "no domain discriminator column");

	let total = spec.columns(Surface::Total);
	assert_eq!(total.len(), 14, "ts + shard + 12 counters; levels must not reach ::total");

	let counters = spec.measures.iter().filter(|m| m.kind == MetricKind::Counter).count();
	let levels = spec.measures.iter().filter(|m| m.kind == MetricKind::Level).count();
	assert_eq!((levels, counters), (10, 12), "the old shards/warms/reads split was exactly levels vs counters");
}
