// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;
use reifydb_core::{interface::catalog::id::NamespaceId, metrics::sample::MetricKind};
use reifydb_sub_metrics::framework::spec::{DomainShape, MetricsDomain, PushKind, Surface};
use reifydb_value::value::value_type::ValueType;

#[test]
fn operator_range_keyspace_current_and_total_are_queryable_after_bootstrap() {
	// Without the namespace bootstrapped under ::range both surfaces fail to register and every query errors.
	let db = TestDb::memory();

	for table in ["current", "total"] {
		assert_eq!(
			db.row_count(&format!("from system::metrics::store::operator::range::keyspace::{table}")),
			0,
			"store::operator::range::keyspace::{table} must be queryable and empty without a range tier",
		);
	}
}

#[test]
fn operator_range_keyspace_is_dimensioned_by_keyspace_name_not_by_shard() {
	// A keyspace spans shards, so a shard dimension would fragment each keyspace into one row per shard.
	let spec = MetricsDomain::StoreOperatorRangeKeyspace.spec();

	assert_eq!(spec.shape, DomainShape::Wide);
	assert_eq!(spec.dimensions.len(), 1, "exactly one dimension");
	assert_eq!(spec.dimensions[0].name, "keyspace");
	// The dimension value is KeyspaceId::name(), never the raw u8, so the column must be Utf8.
	assert_eq!(spec.dimensions[0].data_type, ValueType::Utf8);
	assert!(!spec.dimensions[0].optional, "every row must know its keyspace");
	assert!(spec.dimensions.iter().all(|d| d.name != "shard"), "a keyspace must never carry a shard dimension");
}

#[test]
fn range_and_point_are_two_separate_namespaces() {
	// Two caches with two budgets: sharing a namespace collides on the vtable name and re-merges the
	// tiers into one row set, which is exactly the reading the split exists to take apart.
	let point = MetricsDomain::StoreOperatorPointKeyspace.spec().namespace;
	let range = MetricsDomain::StoreOperatorRangeKeyspace.spec().namespace;

	assert_eq!(point, NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_POINT_KEYSPACE);
	assert_eq!(range, NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_RANGE_KEYSPACE);
	assert_ne!(point, range, "each operator cache surface must own its own namespace");
}

#[test]
fn operator_range_owns_the_partition_count_and_the_point_read_counters() {
	// Residency is no longer the completeness claim, so `partitions` counts resident partitions only and
	// says nothing about what is proven; the claim lives in the coverage index. point_hits/point_misses
	// count point reads a proven span settled, a number only the range tier can produce.
	let domain = MetricsDomain::StoreOperatorRangeKeyspace;
	let spec = domain.spec();
	assert!(spec.measures.iter().any(|m| m.name == "partitions"), "{domain:?} must count its partitions");
	assert!(
		spec.measures.iter().all(|m| m.name != "complete_buckets"),
		"{domain:?} must not publish a residency count dressed as a completeness count"
	);
	for name in ["point_hits", "point_misses"] {
		assert!(spec.measures.iter().any(|m| m.name == name), "{domain:?} must publish {name}");
	}
	for name in ["fills_started", "fills_duplicate", "insertions"] {
		assert!(
			spec.measures.iter().all(|m| m.name != name),
			"{name} is a point-tier counter and must never appear on {domain:?}"
		);
	}
}

#[test]
fn operator_range_keyspace_pins_the_published_layout() {
	// Resident state is a level and cache work is a counter; swapping either publishes a summed gauge.
	let spec = MetricsDomain::StoreOperatorRangeKeyspace.spec();

	let intervals = spec
		.measures
		.iter()
		.find(|m| m.name == "intervals")
		.expect("coverage fragmentation is invisible without a claim count per keyspace");
	assert_eq!(intervals.kind, MetricKind::Level, "a claim count is a level; as a counter it sums across samples");

	let levels = spec.measures.iter().filter(|m| m.kind == MetricKind::Level).count();
	let counters = spec.measures.iter().filter(|m| m.kind == MetricKind::Counter).count();
	assert_eq!((levels, counters), (5, 9), "used/limit/partitions/intervals/entries are levels, the rest counters");
	assert_eq!(spec.measures.len(), 14, "no measure outside the level/counter split");

	let current = spec.columns(Surface::Current);
	assert_eq!(current.len(), 16, "ts + keyspace + 5 levels + 9 counters");
	assert_eq!(current[0].name, "ts");
	assert_eq!(current[1].name, "keyspace");
	assert!(
		current.iter().all(|column| column.kind != MetricKind::Counter),
		"counters must publish as Delta in ::current"
	);

	let total = spec.columns(Surface::Total);
	assert_eq!(total.len(), 11, "ts + keyspace + 9 counters; levels must never reach ::total");
	assert!(!total.iter().any(|column| column.name == "used"), "a level in ::total is a summed gauge");
	assert!(spec.has_total, "cache work is cumulative, so ::total must exist");
}

#[test]
fn operator_range_keyspace_updates_rows_and_writes_no_snapshot_series() {
	// Census retains only the keyspaces in the latest sample, dropping lifetime counters once buckets evict.
	assert_eq!(MetricsDomain::StoreOperatorRangeKeyspace.push_kind(), PushKind::Update);
	// No series id is reserved for this domain, so a snapshot path would write into nothing.
	assert_eq!(MetricsDomain::StoreOperatorRangeKeyspace.snapshots_path(), None);
	assert!(
		MetricsDomain::ALL.contains(&MetricsDomain::StoreOperatorRangeKeyspace),
		"a domain outside ALL is never registered and never sampled"
	);
}
