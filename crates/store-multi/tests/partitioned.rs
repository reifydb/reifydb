// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Storage-layer coverage for the PartitionedSource keyspace: a partitioned row must route to its owner's own partsource
// table, never to the multi table.

use std::sync::Arc;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::EventBus,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{EntryKind, EntryLayout, MultiVersionCommit, MultiVersionGet},
	},
	key::row::PartitionedRowKey,
	lifecycle::watermark::EvictionWatermark,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_commit::{MultiVersionScope, store::CommitStore};
use reifydb_store_multi::{
	config::{CommitStoreConfig, MultiStoreConfig, PersistentConfig},
	store::StandardMultiStore,
	tier::{TierStorage, point::MultiPointConfig, range::MultiRangeConfig},
};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, partition::Partition, row_number::RowNumber},
};

fn encoded_bytes(bytes: &[u8]) -> EncodedBytes {
	EncodedBytes(CowVec::new(bytes.to_vec()))
}

struct FixedWatermark(CommitVersion);

impl EvictionWatermark for FixedWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

#[test]
fn partitioned_rows_route_to_partsource_across_tiers() {
	// A missing partitioned arm in range classification routes range reads to the multi table, which
	// is empty after a flush, so the flushed rows would silently disappear from the scan.
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let (sqlite_config, _guard) = SqliteConfig::in_memory();
	let store = StandardMultiStore::new(MultiStoreConfig {
		commit: CommitStoreConfig {
			storage: CommitStore::new(),
		},
		point: Some(MultiPointConfig::testing()),
		range: Some(MultiRangeConfig::testing()),
		persistent: Some(PersistentConfig::sqlite(sqlite_config)),
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus,
		spawner,
		clock: Clock::Real,
	})
	.unwrap();

	let storage = StorageId::Table(TableId(1));
	let us = Partition::of(&[Value::Utf8("us".to_string())]);
	let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
	let k_us1 = PartitionedRowKey::encoded(storage, us, RowNumber(1));
	let k_eu2 = PartitionedRowKey::encoded(storage, eu, RowNumber(2));
	let k_us3 = PartitionedRowKey::encoded(storage, us, RowNumber(3));

	MultiVersionCommit::commit(
		&store,
		CowVec::new(vec![
			Delta::Set {
				key: k_us1.clone(),
				bytes: encoded_bytes(b"a"),
			},
			Delta::Set {
				key: k_eu2.clone(),
				bytes: encoded_bytes(b"b"),
			},
		]),
		CommitVersion(1),
	)
	.unwrap();
	// Pin the eviction cutoff at v1 so the flush actually moves the v1 rows to the persistent tier.
	store.set_eviction_watermark(Arc::new(FixedWatermark(CommitVersion(1))));
	store.flush_pending_blocking();

	// This third row is deliberately left unflushed so the reads below straddle both tiers.
	MultiVersionCommit::commit(
		&store,
		CowVec::new(vec![Delta::Set {
			key: k_us3.clone(),
			bytes: encoded_bytes(b"c"),
		}]),
		CommitVersion(2),
	)
	.unwrap();

	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(2),
	};

	let all: Vec<_> =
		store.range(PartitionedRowKey::full_scan(storage), scope, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	assert_eq!(all.len(), 3, "full-object range must return flushed + buffered partitioned rows across tiers");

	let us_rows: Vec<_> = store
		.range(PartitionedRowKey::partition_range(storage, us), scope, 1024)
		.collect::<Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(us_rows.len(), 2, "us partition range must return only us rows across tiers");

	assert!(
		store.get(&k_us1, CommitVersion(2)).unwrap().is_some(),
		"flushed partitioned row readable via point get"
	);
	assert!(
		store.get(&k_us3, CommitVersion(2)).unwrap().is_some(),
		"buffered partitioned row readable via point get"
	);

	let persistent = store.persistent().expect("persistent tier configured");
	assert!(
		persistent
			.get(EntryKind::PartitionedSource(storage, EntryLayout::Row), k_us1.as_ref(), CommitVersion(2))
			.unwrap()
			.value()
			.is_some(),
		"flushed partitioned row must live in the partsource_<storage> table"
	);
	assert!(
		persistent.get(EntryKind::Multi, k_us1.as_ref(), CommitVersion(2)).unwrap().value().is_none(),
		"partitioned row must NOT be in the multi table"
	);
}
