// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! True physical delete for `Delta::Drop` on SOURCE keys.
//!
//! Source and partitioned-source drops route through the same evict path as operator
//! drops: all versions leave the commit buffer inside the commit, a pending-drop mask
//! hides the stale persisted row from readers at or above the drop version, and the
//! drop actor purges the SQLite row in the background bounded by the drop version.
//! These tests pin that contract for the Source/PartitionedSource keyspaces; the
//! keep-latest DropActor behavior remains exclusive to `EntryKind::Multi` (covered by
//! the existing store_drop tests).

use std::collections::HashMap;

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::{EntryKind, MultiVersionCommit, MultiVersionGet, classify_key},
	},
	key::{
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::RowKey,
	},
};
use reifydb_store_multi::{MultiVersionScope, store::StandardMultiStore, tier::TierStorage};
use reifydb_value::{
	cow_vec,
	util::cowvec::CowVec,
	value::{Value, partition::Partition, row_number::RowNumber},
};

fn table_row_key(table: u64, row: u64) -> EncodedKey {
	RowKey::encoded(ShapeId::Table(TableId(table)), RowNumber(row))
}

fn partitioned_row_key(table: u64, partition: Partition, row: u64) -> EncodedKey {
	PartitionedRowKey::encoded(ShapeId::Table(TableId(table)), partition, RowLocator::Row(RowNumber(row)))
}

fn persistent_only_set(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	let persistent = store.persistent().expect("persistent tier configured");
	let table = classify_key(k);
	let mut batches: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> = HashMap::new();
	batches.entry(table).or_default().push((k.clone(), Some(CowVec::new(value.as_bytes().to_vec()))));
	persistent.set(CommitVersion(version), batches).unwrap();
}

fn persistent_row(store: &StandardMultiStore, k: &EncodedKey) -> Option<(u64, Vec<u8>)> {
	let persistent = store.persistent().expect("persistent tier configured");
	match persistent.get(classify_key(k), k.as_ref(), CommitVersion(u64::MAX)).unwrap() {
		reifydb_store_multi::tier::VersionedGetResult::Value {
			value,
			version,
		} => Some((version.0, value.to_vec())),
		_ => None,
	}
}

fn wait_until_persistent_gone(store: &StandardMultiStore, k: &EncodedKey) {
	let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
	while persistent_row(store, k).is_some() {
		assert!(
			std::time::Instant::now() < deadline,
			"the drop actor did not purge the persisted row within the deadline"
		);
		std::thread::sleep(std::time::Duration::from_millis(10));
	}
}

fn get(store: &StandardMultiStore, k: &EncodedKey, version: u64) -> Option<Vec<u8>> {
	store.get(k, CommitVersion(version)).unwrap().map(|r| r.row.to_vec())
}

fn range_keys(store: &StandardMultiStore, range: EncodedKeyRange, read: u64) -> Vec<EncodedKey> {
	store.range(
		range,
		MultiVersionScope::AsOf {
			read: CommitVersion(read),
		},
		64,
	)
	.map(|r| r.unwrap().key)
	.collect()
}

#[test]
fn source_drop_masks_immediately_and_purges_persistence_in_the_background() {
	// A source drop must behave like an operator drop: readers at or above the drop
	// version see the key gone the moment the commit returns (point get AND range scan,
	// the latter exercising the widened mask gate), the commit path performs no SQLite
	// work, and the drop actor purges the row in the background. After the purge, the
	// row is physically gone for every reader at or below the drop version.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let shape = ShapeId::Table(TableId(1));
	let k1 = table_row_key(1, 1);
	let k2 = table_row_key(1, 2);
	persistent_only_set(&store, &k1, 5, "v5-a");
	persistent_only_set(&store, &k2, 5, "v5-b");

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Drop {
			key: k1.clone(),
		}],
		CommitVersion(8),
	)
	.unwrap();

	assert_eq!(get(&store, &k1, 9), None, "the dropped key must read as gone immediately after commit");
	let keys = range_keys(&store, RowKey::full_scan(shape), 9);
	assert!(!keys.contains(&k1), "a range scan at read >= drop version must mask the persisted row");
	assert!(keys.contains(&k2), "the untouched sibling row must stay visible in the range");
	assert!(
		persistent_row(&store, &k1).is_some(),
		"the commit path must not touch SQLite; the stale row is masked, not deleted"
	);

	wait_until_persistent_gone(&store, &k1);

	assert_eq!(get(&store, &k1, 9), None);
	let keys = range_keys(&store, RowKey::full_scan(shape), 9);
	assert!(!keys.contains(&k1));
	assert!(keys.contains(&k2));
	assert_eq!(
		get(&store, &k1, 5),
		None,
		"after the purge, readers at or below the drop version have physically lost the row"
	);
}

#[test]
fn source_drop_then_reinsert_survives_the_background_purge() {
	// A key dropped at v8 and recreated at v10 must survive the deferred purge, which
	// is bounded by the drop version. The sentinel shares the drop commit, so its
	// disappearance proves the purge batch containing both keys has run.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = table_row_key(1, 1);
	let sentinel = table_row_key(1, 2);
	persistent_only_set(&store, &k, 5, "old");
	persistent_only_set(&store, &sentinel, 5, "old");

	MultiVersionCommit::commit(
		&store,
		cow_vec![
			Delta::Drop {
				key: k.clone(),
			},
			Delta::Drop {
				key: sentinel.clone(),
			}
		],
		CommitVersion(8),
	)
	.unwrap();
	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"new".to_vec())),
		}],
		CommitVersion(10),
	)
	.unwrap();

	assert_eq!(get(&store, &k, 9), None, "a reader pinned between drop and recreate sees the mask");
	assert_eq!(get(&store, &k, 15).as_deref(), Some(b"new".as_slice()));

	persistent_only_set(&store, &k, 10, "new");

	wait_until_persistent_gone(&store, &sentinel);

	assert_eq!(
		persistent_row(&store, &k),
		Some((10, b"new".to_vec())),
		"the deferred purge must not remove a row newer than the drop version"
	);
	assert_eq!(get(&store, &k, 15).as_deref(), Some(b"new".as_slice()));
}

#[test]
fn source_purge_is_bounded_by_the_drop_version() {
	// The purge primitive over the source SQLite table: deleting through the drop
	// version must leave a newer row alone.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let persistent = store.persistent().expect("persistent tier configured");
	let k = table_row_key(1, 7);
	persistent_only_set(&store, &k, 10, "newer");

	let purged = persistent
		.delete_keys_through(classify_key(&k), std::slice::from_ref(&(k.clone(), CommitVersion(8))))
		.unwrap();
	assert_eq!(purged, 0, "a row newer than the purge bound must survive");
	assert_eq!(persistent_row(&store, &k), Some((10, b"newer".to_vec())));

	let purged = persistent
		.delete_keys_through(classify_key(&k), std::slice::from_ref(&(k.clone(), CommitVersion(10))))
		.unwrap();
	assert_eq!(purged, 1, "a row at the purge bound is dropped state and must go");
	assert_eq!(persistent_row(&store, &k), None);
}

#[test]
fn partitioned_source_drop_masks_and_purges() {
	// Same contract for the PartitionedSource keyspace: the dropped partition row is
	// masked out of partition range scans immediately and purged in the background,
	// while the sibling partition stays intact.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let shape = ShapeId::Table(TableId(2));
	let us = Partition::of(&[Value::Utf8("us".to_string())]);
	let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
	let k_us = partitioned_row_key(2, us, 1);
	let k_eu = partitioned_row_key(2, eu, 2);
	persistent_only_set(&store, &k_us, 5, "us-row");
	persistent_only_set(&store, &k_eu, 5, "eu-row");

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Drop {
			key: k_us.clone(),
		}],
		CommitVersion(8),
	)
	.unwrap();

	assert_eq!(get(&store, &k_us, 9), None);
	let us_keys = range_keys(&store, PartitionedRowKey::partition_range(shape, us), 9);
	assert!(us_keys.is_empty(), "the dropped partition row must be masked out of the partition range scan");
	let eu_keys = range_keys(&store, PartitionedRowKey::partition_range(shape, eu), 9);
	assert!(eu_keys.contains(&k_eu), "the sibling partition must be unaffected");
	assert!(persistent_row(&store, &k_us).is_some());

	wait_until_persistent_gone(&store, &k_us);

	assert!(range_keys(&store, PartitionedRowKey::partition_range(shape, us), 9).is_empty());
	assert!(range_keys(&store, PartitionedRowKey::partition_range(shape, eu), 9).contains(&k_eu));
}

#[test]
fn committed_source_drop_removes_all_versions_without_persistence() {
	// Memory-only store: a source drop physically removes every version from the
	// commit buffer inside the commit, leaving no tombstone and no trace at any read
	// version.
	let store = StandardMultiStore::testing_memory();
	let shape = ShapeId::Table(TableId(3));
	let k = table_row_key(3, 1);

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"v1".to_vec())),
		}],
		CommitVersion(1),
	)
	.unwrap();
	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"v2".to_vec())),
		}],
		CommitVersion(2),
	)
	.unwrap();
	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Drop {
			key: k.clone(),
		}],
		CommitVersion(5),
	)
	.unwrap();

	assert_eq!(get(&store, &k, 10), None, "the dropped key must be gone above the drop version");
	assert_eq!(get(&store, &k, 2), None, "all historical versions must be physically gone");
	assert_eq!(get(&store, &k, 1), None);
	assert!(range_keys(&store, RowKey::full_scan(shape), 10).is_empty());
}
