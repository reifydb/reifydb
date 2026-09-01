// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;
use reifydb_core::{interface::catalog::id::NamespaceId, metrics::sample::MetricKind};
use reifydb_sub_metrics::framework::spec::{DomainShape, MetricsDomain, PushKind, Surface};
use reifydb_value::value::value_type::ValueType;

#[test]
fn operator_point_keyspace_current_and_total_are_queryable_after_bootstrap() {
	// Without the namespace bootstrapped under ::point both surfaces fail to register and every query errors.
	let db = TestDb::memory();

	for table in ["current", "total"] {
		assert_eq!(
			db.row_count(&format!("from system::metrics::store::operator::point::keyspace::{table}")),
			0,
			"store::operator::point::keyspace::{table} must be queryable and empty without a point tier",
		);
	}
}

#[test]
fn operator_point_keyspace_is_dimensioned_by_keyspace_name_not_by_shard() {
	// A keyspace spans shards, so a shard dimension would fragment each keyspace into one row per shard.
	let spec = MetricsDomain::StoreOperatorPointKeyspace.spec();

	assert_eq!(spec.shape, DomainShape::Wide);
	assert_eq!(spec.dimensions.len(), 1, "exactly one dimension");
	assert_eq!(spec.dimensions[0].name, "keyspace");
	// The dimension value is KeyspaceId::name(), never the raw u8, so the column must be Utf8.
	assert_eq!(spec.dimensions[0].data_type, ValueType::Utf8);
	assert!(!spec.dimensions[0].optional, "every row must know its keyspace");
	assert!(spec.dimensions.iter().all(|d| d.name != "shard"), "a keyspace must never carry a shard dimension");
}

#[test]
fn operator_point_carries_no_bucket_measure_on_its_surface() {
	// The point tier keys on the whole inner key and is flat; a `buckets` count here would publish a
	// number no structure produces, and would re-merge the point rows with the range tier's shape.
	let domain = MetricsDomain::StoreOperatorPointKeyspace;
	let spec = domain.spec();
	assert!(spec.measures.iter().all(|m| m.name != "partitions"), "{domain:?} must own no partition count");
	assert!(
		spec.measures.iter().any(|m| m.name == "insertions"),
		"{domain:?} must publish insertions, which only the point tier counts"
	);
	assert_eq!(spec.namespace, NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_POINT_KEYSPACE);
}

#[test]
fn operator_point_keyspace_pins_the_published_layout() {
	// Resident state is a level and cache work is a counter; swapping either publishes a summed gauge.
	let spec = MetricsDomain::StoreOperatorPointKeyspace.spec();

	let levels = spec.measures.iter().filter(|m| m.kind == MetricKind::Level).count();
	let counters = spec.measures.iter().filter(|m| m.kind == MetricKind::Counter).count();
	assert_eq!((levels, counters), (3, 7), "used/limit/entries are levels, the rest counters");
	assert_eq!(spec.measures.len(), 10, "no measure outside the level/counter split");

	let current = spec.columns(Surface::Current);
	assert_eq!(current.len(), 12, "ts + keyspace + 3 levels + 7 counters");
	assert_eq!(current[0].name, "ts");
	assert_eq!(current[1].name, "keyspace");
	assert!(
		current.iter().all(|column| column.kind != MetricKind::Counter),
		"counters must publish as Delta in ::current"
	);

	let total = spec.columns(Surface::Total);
	assert_eq!(total.len(), 9, "ts + keyspace + 7 counters; levels must never reach ::total");
	assert!(!total.iter().any(|column| column.name == "used"), "a level in ::total is a summed gauge");
	assert!(spec.has_total, "cache work is cumulative, so ::total must exist");
}

#[test]
fn operator_point_keyspace_updates_rows_and_writes_no_snapshot_series() {
	// Census retains only the keyspaces in the latest sample, dropping lifetime counters once entries evict.
	assert_eq!(MetricsDomain::StoreOperatorPointKeyspace.push_kind(), PushKind::Update);
	// No series id is reserved for this domain, so a snapshot path would write into nothing.
	assert_eq!(MetricsDomain::StoreOperatorPointKeyspace.snapshots_path(), None);
	assert!(
		MetricsDomain::ALL.contains(&MetricsDomain::StoreOperatorPointKeyspace),
		"a domain outside ALL is never registered and never sampled"
	);
}
