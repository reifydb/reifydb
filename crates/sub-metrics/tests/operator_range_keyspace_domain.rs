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
	// The dimension value is Keyspace::name(), never the raw u8, so the column must be Utf8.
	assert_eq!(spec.dimensions[0].data_type, ValueType::Utf8);
	assert!(!spec.dimensions[0].optional, "every row must know its keyspace");
	assert!(spec.dimensions.iter().all(|d| d.name != "shard"), "a keyspace must never carry a shard dimension");
}

#[test]
fn operator_range_keyspace_drops_the_per_shard_limit_measure() {
	// `limit` is a per-shard budget and means nothing per keyspace, which is why this is its own domain.
	let spec = MetricsDomain::StoreOperatorRangeKeyspace.spec();
	assert!(spec.measures.iter().all(|m| m.name != "limit"), "no per-shard budget on a keyspace row");

	let shard_spec = MetricsDomain::StoreOperatorRange.spec();
	assert!(shard_spec.measures.iter().any(|m| m.name == "limit"), "the shard domain still owns limit");
	assert_ne!(spec.namespace, shard_spec.namespace, "keyspace rows must not merge into the shard namespace");
	assert_eq!(spec.namespace, NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_RANGE_KEYSPACE);
}

#[test]
fn range_and_point_are_four_separate_namespaces() {
	// Two caches with two budgets: sharing a namespace collides on the vtable name and re-merges the
	// tiers into one row set, which is exactly the reading the split exists to take apart.
	let namespaces: Vec<NamespaceId> = [
		MetricsDomain::StoreOperatorPoint,
		MetricsDomain::StoreOperatorPointKeyspace,
		MetricsDomain::StoreOperatorRange,
		MetricsDomain::StoreOperatorRangeKeyspace,
	]
	.map(|domain| domain.spec().namespace)
	.to_vec();

	let mut unique = namespaces.clone();
	unique.sort();
	unique.dedup();
	assert_eq!(unique.len(), 4, "each operator cache surface must own its own namespace");
}

#[test]
fn operator_range_owns_the_partition_count_and_the_point_read_counters() {
	// Residency is no longer the completeness claim, so `partitions` counts resident partitions only and
	// says nothing about what is proven; the claim lives in the coverage index. point_hits/point_misses
	// count point reads a proven span settled, a number only the range tier can produce.
	for domain in [MetricsDomain::StoreOperatorRange, MetricsDomain::StoreOperatorRangeKeyspace] {
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
}

#[test]
fn operator_range_keyspace_pins_the_published_layout() {
	// Resident state is a level and cache work is a counter; swapping either publishes a summed gauge.
	let spec = MetricsDomain::StoreOperatorRangeKeyspace.spec();

	let levels = spec.measures.iter().filter(|m| m.kind == MetricKind::Level).count();
	let counters = spec.measures.iter().filter(|m| m.kind == MetricKind::Counter).count();
	assert_eq!((levels, counters), (3, 8), "used/partitions/entries are levels, the rest counters");
	assert_eq!(spec.measures.len(), 11, "no measure outside the level/counter split");

	let current = spec.columns(Surface::Current);
	assert_eq!(current.len(), 13, "ts + keyspace + 3 levels + 8 counters");
	assert_eq!(current[0].name, "ts");
	assert_eq!(current[1].name, "keyspace");
	assert!(
		current.iter().all(|column| column.kind != MetricKind::Counter),
		"counters must publish as Delta in ::current"
	);

	let total = spec.columns(Surface::Total);
	assert_eq!(total.len(), 10, "ts + keyspace + 8 counters; levels must never reach ::total");
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
	assert!(
		MetricsDomain::ALL.contains(&MetricsDomain::StoreOperatorRange),
		"a domain outside ALL is never registered and never sampled"
	);
}
