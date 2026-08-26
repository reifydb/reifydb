// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi point-tier metrics: catalog surface and vtable registration, driven through the wired subsystem.
//!
//! A bare in-memory database has no point tier, so `store::multi::point::current` and `::total` must be
//! queryable by their RQL paths and empty rather than error. The column layout is pinned against the
//! DomainSpec so the tier's own read vocabulary cannot silently collapse into the shared tier's.

use reifydb::testing::db::TestDb;
use reifydb_core::metrics::sample::MetricKind;
use reifydb_sub_metrics::framework::spec::{MetricsDomain, Surface};

#[test]
fn multi_point_current_and_total_are_queryable_after_bootstrap() {
	let db = TestDb::memory();

	for table in ["current", "total"] {
		assert_eq!(
			db.row_count(&format!("from system::metrics::store::multi::point::{table}")),
			0,
			"store::multi::point::{table} must be queryable and empty for a store without a point tier",
		);
	}
}

#[test]
fn multi_point_spec_separates_a_displaced_hit_from_a_current_one() {
	// previous_hits is the whole reason this tier keeps two versions. Folded into hits it stops being
	// observable, and a reader the second slot rescued reads the same as one the first answered.
	let spec = MetricsDomain::StoreMultiPoint.spec();

	let current = spec.columns(Surface::Current);
	assert_eq!(current.len(), 13, "ts + shard + 3 levels + 8 counters");
	assert_eq!(current[0].name, "ts");
	assert_eq!(current[1].name, "shard");

	let total = spec.columns(Surface::Total);
	assert_eq!(total.len(), 10, "ts + shard + 8 counters; levels must not reach ::total");

	for read in ["hits", "previous_hits", "misses"] {
		assert!(
			spec.measures.iter().any(|measure| measure.name == read),
			"{read} is one of the three outcomes a multi point read has, and all three must be reported apart",
		);
	}
	assert!(
		spec.measures.iter().all(|measure| !measure.name.contains("page")),
		"a page word here means this domain inherited the layer it replaces",
	);

	let counters = spec.measures.iter().filter(|m| m.kind == MetricKind::Counter).count();
	let levels = spec.measures.iter().filter(|m| m.kind == MetricKind::Level).count();
	assert_eq!((levels, counters), (3, 8));
}
