// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Storage-layer coverage for the PartitionedSource keyspace: partitioned rows (KeyKind::PartitionedRow)
// route to EntryKind::PartitionedSource(shape) -> a dedicated partsource_<shape> persistent table, and
// the row-TTL GC scanner reaches them through that keyspace.

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::EventBus,
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::{EntryKind, MultiVersionCommit, MultiVersionGet},
	},
	key::partitioned_row::{PartitionedRowKey, RowLocator},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_multi::{
	MultiVersionScope,
	config::{CommitBufferConfig, MultiStoreConfig, PersistentConfig},
	gc::{
		EvictionWatermark,
		row::{
			ScanStats,
			scanner::{
				ScanResult::{Exhausted, Yielded},
				drop_expired_keys, scan_shape_expired,
			},
		},
	},
	store::StandardMultiStore,
	tier::{RangeCursor, TierStorage, commit::buffer::MultiCommitBufferTier},
};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, partition::Partition, row_number::RowNumber},
};

fn row(bytes: &[u8]) -> EncodedRow {
	EncodedRow(CowVec::new(bytes.to_vec()))
}

struct FixedWatermark(CommitVersion);

impl EvictionWatermark for FixedWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

/// Partitioned rows must survive a flush to persistent AND stay readable via range/point reads that
/// union the commit buffer and the persistent tier. The discriminating case is `classify_range`: a
/// missing partitioned-range arm would route range reads to the `multi` table (empty after flush) and
/// silently drop the flushed rows.
#[test]
fn partitioned_rows_route_to_partsource_across_tiers() {
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let (sqlite_config, _guard) = SqliteConfig::in_memory();
	let store = StandardMultiStore::new(MultiStoreConfig {
		commit: Some(CommitBufferConfig {
			storage: MultiCommitBufferTier::memory(),
		}),
		persistent: Some(PersistentConfig::sqlite(sqlite_config)),
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus,
		spawner,
		clock: Clock::Real,
	})
	.unwrap();

	let shape = ShapeId::Table(TableId(1));
	let us = Partition::of(&[Value::Utf8("us".to_string())]);
	let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
	let k_us1 = PartitionedRowKey::encoded(shape, us, RowLocator::Row(RowNumber(1)));
	let k_eu2 = PartitionedRowKey::encoded(shape, eu, RowLocator::Row(RowNumber(2)));
	let k_us3 = PartitionedRowKey::encoded(shape, us, RowLocator::Row(RowNumber(3)));

	// Commit two partitioned rows and flush them to the persistent tier.
	MultiVersionCommit::commit(
		&store,
		CowVec::new(vec![
			Delta::Set {
				key: k_us1.clone(),
				row: row(b"a"),
			},
			Delta::Set {
				key: k_eu2.clone(),
				row: row(b"b"),
			},
		]),
		CommitVersion(1),
	)
	.unwrap();
	// Pin the eviction cutoff at v1 so the flush actually moves the v1 rows to the persistent tier.
	store.set_eviction_watermark(Arc::new(FixedWatermark(CommitVersion(1))));
	store.flush_pending_blocking();

	// A third partitioned row stays in the commit buffer (no flush).
	MultiVersionCommit::commit(
		&store,
		CowVec::new(vec![Delta::Set {
			key: k_us3.clone(),
			row: row(b"c"),
		}]),
		CommitVersion(2),
	)
	.unwrap();

	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(2),
	};

	// Full-shape range must union flushed (us1, eu2) + buffered (us3).
	let all: Vec<_> =
		store.range(PartitionedRowKey::full_scan(shape), scope, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	assert_eq!(all.len(), 3, "full-shape range must return flushed + buffered partitioned rows across tiers");

	// Single-partition range prunes to the two us rows (us1 flushed + us3 buffered), not eu2.
	let us_rows: Vec<_> = store
		.range(PartitionedRowKey::partition_range(shape, us), scope, 1024)
		.collect::<Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(us_rows.len(), 2, "us partition range must return only us rows across tiers");

	// Point reads across tiers.
	assert!(
		store.get(&k_us1, CommitVersion(2)).unwrap().is_some(),
		"flushed partitioned row readable via point get"
	);
	assert!(
		store.get(&k_us3, CommitVersion(2)).unwrap().is_some(),
		"buffered partitioned row readable via point get"
	);

	// Physical placement: flushed row is in partsource_<shape>, NOT in the shared multi table.
	let persistent = store.persistent().expect("persistent tier configured");
	assert!(
		persistent
			.get(EntryKind::PartitionedSource(shape), k_us1.as_ref(), CommitVersion(2))
			.unwrap()
			.value()
			.is_some(),
		"flushed partitioned row must live in the partsource_<shape> table"
	);
	assert!(
		persistent.get(EntryKind::Multi, k_us1.as_ref(), CommitVersion(2)).unwrap().value().is_none(),
		"partitioned row must NOT be in the multi table"
	);
}

/// The row-TTL scanner reaches partitioned rows only through the PartitionedSource keyspace; the same
/// shape's Source (RowKey, 0x03) scan must not see them.
#[test]
fn partitioned_rows_expire_via_partitioned_source_keyspace() {
	let storage = MultiCommitBufferTier::memory();
	let shape = ShapeId::Table(TableId(1));
	let part_us = Partition::of(&[Value::Utf8("us".to_string())]);
	let part_eu = Partition::of(&[Value::Utf8("eu".to_string())]);
	let table = EntryKind::PartitionedSource(shape);

	for (p, rn) in [(part_us, 1u64), (part_us, 2), (part_eu, 3)] {
		let key = PartitionedRowKey::encoded(shape, p, RowLocator::Row(RowNumber(rn)));
		storage.set(CommitVersion(5), HashMap::from([(table, vec![(key, Some(CowVec::new(b"x".to_vec())))])]))
			.unwrap();
	}

	// The Source-keyspace scan of the same shape must find nothing: partitioned rows live under 0x50.
	let mut cursor = RangeCursor::new();
	let (src_expired, _) =
		scan_shape_expired(&storage, EntryKind::Source(shape), CommitVersion(10), 1024, &mut cursor).unwrap();
	assert!(src_expired.is_empty(), "partitioned rows must NOT appear in the Source keyspace scan");

	// PartitionedSource scan with cutoff >= write version finds all 3 across both partitions and drops them.
	let mut cursor = RangeCursor::new();
	let mut stats = ScanStats::default();
	let mut total = 0u64;
	loop {
		let (expired, result) =
			scan_shape_expired(&storage, table, CommitVersion(10), 1024, &mut cursor).unwrap();
		total += expired.len() as u64;
		if !expired.is_empty() {
			drop_expired_keys(&storage, &expired, &mut stats).unwrap();
		}
		match result {
			Yielded => continue,
			Exhausted => break,
		}
	}
	assert_eq!(total, 3, "all 3 partitioned rows (both partitions) must be found in the partsource keyspace");
	assert_eq!(
		storage.count_current(table).unwrap(),
		0,
		"expired partitioned rows dropped from the partsource table"
	);
}

/// A partitioned row written above the TTL cutoff version must survive.
#[test]
fn young_partitioned_rows_survive_cutoff() {
	let storage = MultiCommitBufferTier::memory();
	let shape = ShapeId::Table(TableId(2));
	let table = EntryKind::PartitionedSource(shape);
	let p = Partition::of(&[Value::Utf8("us".to_string())]);
	let key = PartitionedRowKey::encoded(shape, p, RowLocator::Row(RowNumber(1)));
	storage.set(CommitVersion(20), HashMap::from([(table, vec![(key, Some(CowVec::new(b"x".to_vec())))])]))
		.unwrap();

	let mut cursor = RangeCursor::new();
	let (expired, _) = scan_shape_expired(&storage, table, CommitVersion(10), 1024, &mut cursor).unwrap();
	assert!(expired.is_empty(), "a row written above the cutoff version must survive");
	assert_eq!(storage.count_current(table).unwrap(), 1);
}
