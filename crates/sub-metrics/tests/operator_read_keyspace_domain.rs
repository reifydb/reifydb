// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;
use reifydb_core::{interface::catalog::id::NamespaceId, metrics::sample::MetricKind};
use reifydb_sub_metrics::framework::spec::{DomainShape, MetricsDomain, PushKind, Surface};
use reifydb_value::value::value_type::ValueType;

#[test]
fn operator_read_keyspace_current_and_total_are_queryable_after_bootstrap() {
	// Without the namespace bootstrapped under ::read both surfaces fail to register and every query errors.
	let db = TestDb::memory();

	for table in ["current", "total"] {
		assert_eq!(
			db.row_count(&format!("from system::metrics::store::operator::read::keyspace::{table}")),
			0,
			"store::operator::read::keyspace::{table} must be queryable and empty without a read tier",
		);
	}
}

#[test]
fn operator_read_keyspace_is_dimensioned_by_keyspace_name_not_by_shard() {
	// A keyspace spans shards, so a shard dimension would fragment each keyspace into one row per shard.
	let spec = MetricsDomain::StoreOperatorReadKeyspace.spec();

	assert_eq!(spec.shape, DomainShape::Wide);
	assert_eq!(spec.dimensions.len(), 1, "exactly one dimension");
	assert_eq!(spec.dimensions[0].name, "keyspace");
	// The dimension value is Keyspace::name(), never the raw u8, so the column must be Utf8.
	assert_eq!(spec.dimensions[0].data_type, ValueType::Utf8);
	assert!(!spec.dimensions[0].optional, "every row must know its keyspace");
	assert!(spec.dimensions.iter().all(|d| d.name != "shard"), "a keyspace must never carry a shard dimension");
}

#[test]
fn operator_read_keyspace_drops_the_per_shard_limit_measure() {
	// `limit` is a per-shard budget and means nothing per keyspace, which is why this is its own domain.
	let spec = MetricsDomain::StoreOperatorReadKeyspace.spec();
	assert!(spec.measures.iter().all(|m| m.name != "limit"), "no per-shard budget on a keyspace row");

	let shard_spec = MetricsDomain::StoreOperatorRead.spec();
	assert!(shard_spec.measures.iter().any(|m| m.name == "limit"), "the shard domain still owns limit");
	assert_ne!(spec.namespace, shard_spec.namespace, "keyspace rows must not merge into the shard namespace");
	assert_eq!(spec.namespace, NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_READ_KEYSPACE);
}

#[test]
fn operator_read_keyspace_pins_the_published_layout() {
	// Resident state is a level and cache work is a counter; swapping either publishes a summed gauge.
	let spec = MetricsDomain::StoreOperatorReadKeyspace.spec();

	let levels = spec.measures.iter().filter(|m| m.kind == MetricKind::Level).count();
	let counters = spec.measures.iter().filter(|m| m.kind == MetricKind::Counter).count();
	assert_eq!((levels, counters), (3, 6), "used/buckets/entries are levels, the rest counters");
	assert_eq!(spec.measures.len(), 9, "no measure outside the level/counter split");

	let current = spec.columns(Surface::Current);
	assert_eq!(current.len(), 11, "ts + keyspace + 3 levels + 6 counters");
	assert_eq!(current[0].name, "ts");
	assert_eq!(current[1].name, "keyspace");
	assert!(
		current.iter().all(|column| column.kind != MetricKind::Counter),
		"counters must publish as Delta in ::current"
	);

	let total = spec.columns(Surface::Total);
	assert_eq!(total.len(), 8, "ts + keyspace + 6 counters; levels must never reach ::total");
	assert!(!total.iter().any(|column| column.name == "used"), "a level in ::total is a summed gauge");
	assert!(spec.has_total, "cache work is cumulative, so ::total must exist");
}

#[test]
fn operator_read_keyspace_updates_rows_and_writes_no_snapshot_series() {
	// Census retains only the keyspaces in the latest sample, dropping lifetime counters once buckets evict.
	assert_eq!(MetricsDomain::StoreOperatorReadKeyspace.push_kind(), PushKind::Update);
	// No series id is reserved for this domain, so a snapshot path would write into nothing.
	assert_eq!(MetricsDomain::StoreOperatorReadKeyspace.snapshots_path(), None);
	assert!(
		MetricsDomain::ALL.contains(&MetricsDomain::StoreOperatorReadKeyspace),
		"a domain outside ALL is never registered and never sampled"
	);
}
