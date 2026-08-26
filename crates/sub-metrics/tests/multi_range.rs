// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi range-tier metrics: catalog surface and vtable registration, driven through the wired subsystem.
//!
//! A bare in-memory database has no range tier, so `store::multi::range::current` and `::total` must be
//! queryable by their RQL paths and empty rather than error. The column layout is pinned against the
//! DomainSpec so the split from the read buffer cannot silently drift back into one surface.

use reifydb::testing::db::TestDb;
use reifydb_core::metrics::sample::MetricKind;
use reifydb_sub_metrics::framework::spec::{MetricsDomain, Surface};

#[test]
fn multi_range_current_and_total_are_queryable_after_bootstrap() {
	let db = TestDb::memory();

	for table in ["current", "total"] {
		assert_eq!(
			db.row_count(&format!("from system::metrics::store::multi::range::{table}")),
			0,
			"store::multi::range::{table} must be queryable and empty for a store without a range tier",
		);
	}
}

#[test]
fn multi_range_spec_pins_the_layout_the_read_buffer_gave_up() {
	// Every measure the read buffer shed must land here, or the split drops a counter instead of moving it.
	let spec = MetricsDomain::StoreMultiRange.spec();

	let current = spec.columns(Surface::Current);
	assert_eq!(current.len(), 19, "ts + shard + 5 levels + 12 counters");
	assert_eq!(current[0].name, "ts");
	assert_eq!(current[1].name, "shard");
	assert!(current.iter().all(|column| column.name != "domain"), "no domain discriminator column");

	let total = spec.columns(Surface::Total);
	assert_eq!(total.len(), 14, "ts + shard + 12 counters; levels must not reach ::total");

	for moved in ["materializes", "materializes_refused"] {
		assert!(
			spec.measures.iter().any(|measure| measure.name == moved),
			"{moved} left the read buffer and must be reported here, or the split lost it",
		);
	}
	for renamed in ["hits", "misses", "complete_partitions"] {
		assert!(
			spec.measures.iter().any(|measure| measure.name == renamed),
			"{renamed} carries what the read buffer named in page words and must exist under the tier vocabulary",
		);
	}
	assert!(
		spec.measures.iter().all(|measure| !measure.name.contains("page")),
		"a range tier reports partitions, so a page word here means the split kept the layer it replaced",
	);

	let counters = spec.measures.iter().filter(|m| m.kind == MetricKind::Counter).count();
	let levels = spec.measures.iter().filter(|m| m.kind == MetricKind::Level).count();
	assert_eq!((levels, counters), (5, 12));
}
