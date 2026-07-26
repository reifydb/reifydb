// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Removal semantics for SOURCE keys after the deletion unification.
//!
//! These tests replace the former `source_drop.rs`, which pinned the retired `Delta::Drop` model:
//! all versions left the commit buffer inside the commit, a RAM-resident pending-drop mask hid the
//! stale persisted row, and a background actor purged SQLite later. None of that exists now. A
//! removal is a tombstone written through the ordinary commit path, so the contract inverts in one
//! important place: a drop erased the key for EVERY reader, whereas a tombstone only hides it from
//! readers at or above the tombstone's version. Snapshot isolation below that version is preserved,
//! and these tests exist to keep it that way.

use std::collections::HashMap;

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, object::ObjectId},
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
	RowKey::encoded(ObjectId::Table(TableId(table)), RowNumber(row))
}

fn partitioned_row_key(table: u64, partition: Partition, row: u64) -> EncodedKey {
	PartitionedRowKey::encoded(ObjectId::Table(TableId(table)), partition, RowLocator::Row(RowNumber(row)))
}

/// Seeds the persistent tier directly, bypassing the commit buffer, so a test can set up a row that
/// exists only on disk and then observe how a later in-buffer tombstone interacts with it.
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
fn source_removal_hides_the_key_at_or_above_its_version_only() {
	// The core inversion. Under the retired drop model the row was erased for every reader and the
	// assertion here was `get(v5) == None`. A tombstone is version-scoped: the reader pinned at v5
	// committed before the removal existed and must still see what it saw. If this ever starts
	// returning None, removals have stopped respecting snapshot isolation and every long-running
	// query pinned below a removal silently loses rows.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let object = ObjectId::Table(TableId(1));
	let k1 = table_row_key(1, 1);
	let k2 = table_row_key(1, 2);
	persistent_only_set(&store, &k1, 5, "v5-a");
	persistent_only_set(&store, &k2, 5, "v5-b");

	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k1.clone())], CommitVersion(8)).unwrap();

	assert_eq!(get(&store, &k1, 9), None, "a reader above the tombstone must not see the removed row");
	let keys = range_keys(&store, RowKey::full_scan(object), 9);
	assert!(!keys.contains(&k1), "the range scan above the tombstone must not surface the persisted row");
	assert!(keys.contains(&k2), "the untouched sibling row must stay visible in the range");

	assert_eq!(
		get(&store, &k1, 5).as_deref(),
		Some(b"v5-a".as_slice()),
		"a reader pinned below the tombstone must still see the row it was able to see when it started"
	);
	let keys = range_keys(&store, RowKey::full_scan(object), 5);
	assert!(keys.contains(&k1), "the range scan below the tombstone must still surface the row");
}

#[test]
fn source_removal_leaves_the_persisted_row_for_the_reaper_not_the_commit() {
	// The commit path writes a tombstone and stops. It does not reach into SQLite, and nothing
	// purges synchronously. The prior row legitimately stays on disk as history below the tombstone;
	// collecting it is the tombstone reaper's job, on its own schedule and behind its own floor.
	// This pins that the commit is cheap, which is the reason the eviction path can keep up at all.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = table_row_key(1, 1);
	persistent_only_set(&store, &k, 5, "v5-a");

	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k.clone())], CommitVersion(8)).unwrap();

	assert_eq!(
		persistent_row(&store, &k),
		Some((5, b"v5-a".to_vec())),
		"the commit must not delete from SQLite; the prior version stays as history below the tombstone"
	);
	assert_eq!(get(&store, &k, 9), None, "and the row is still invisible above the tombstone regardless");
}

#[test]
fn source_removal_then_reinsert_resolves_per_read_version() {
	// A key removed at v8 and written again at v10 has three distinct answers depending on where the
	// reader is pinned. The retired model could not express the v5 case at all, because the drop had
	// erased the row outright. Getting any one of these three wrong is a correctness break that a
	// single-version assertion would not catch.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = table_row_key(1, 1);
	persistent_only_set(&store, &k, 5, "old");

	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k.clone())], CommitVersion(8)).unwrap();
	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"new".to_vec())),
		}],
		CommitVersion(10),
	)
	.unwrap();

	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"old".as_slice()), "below the tombstone: the original row");
	assert_eq!(get(&store, &k, 9), None, "between tombstone and reinsert: gone");
	assert_eq!(get(&store, &k, 15).as_deref(), Some(b"new".as_slice()), "above the reinsert: the new row");
}

#[test]
fn partitioned_source_removal_hides_only_the_removed_partition_row() {
	// Same contract on the PartitionedSource keyspace, which reaches readers through partition range
	// scans rather than full scans. The sibling partition is the control: a removal must never widen
	// past the key it names.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let object = ObjectId::Table(TableId(2));
	let us = Partition::of(&[Value::Utf8("us".to_string())]);
	let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
	let k_us = partitioned_row_key(2, us, 1);
	let k_eu = partitioned_row_key(2, eu, 2);
	persistent_only_set(&store, &k_us, 5, "us-row");
	persistent_only_set(&store, &k_eu, 5, "eu-row");

	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k_us.clone())], CommitVersion(8)).unwrap();

	assert_eq!(get(&store, &k_us, 9), None);
	assert!(
		range_keys(&store, PartitionedRowKey::partition_range(object, us), 9).is_empty(),
		"the removed partition row must not surface above the tombstone"
	);
	assert!(
		range_keys(&store, PartitionedRowKey::partition_range(object, eu), 9).contains(&k_eu),
		"the sibling partition must be unaffected"
	);
	assert!(
		range_keys(&store, PartitionedRowKey::partition_range(object, us), 5).contains(&k_us),
		"and below the tombstone the removed partition row is still there"
	);
}

#[test]
fn memory_only_source_removal_keeps_every_version_below_the_tombstone() {
	// Buffer-only store, no persistence in play, so this isolates the commit buffer's own version
	// resolution. The retired test asserted that v1 and v2 were physically gone after the drop; under
	// unification they are retained as history and only become invisible from v5 upward. Reclaiming
	// them is the reaper's business, not the commit's.
	let store = StandardMultiStore::testing_memory();
	let object = ObjectId::Table(TableId(3));
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
	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k.clone())], CommitVersion(5)).unwrap();

	assert_eq!(get(&store, &k, 10), None, "above the tombstone the key is gone");
	assert_eq!(get(&store, &k, 2).as_deref(), Some(b"v2".as_slice()), "v2 remains readable at its own version");
	assert_eq!(get(&store, &k, 1).as_deref(), Some(b"v1".as_slice()), "v1 remains readable at its own version");
	assert!(range_keys(&store, RowKey::full_scan(object), 10).is_empty());
	assert!(range_keys(&store, RowKey::full_scan(object), 2).contains(&k));
}

#[test]
fn repeated_removal_of_an_already_removed_key_is_accepted_and_stays_removed() {
	// The evictor is not exactly-once: a key can be handed to it again before the reaper has
	// collected the first tombstone. Removing an already-removed key must be a well-formed write
	// rather than an error or a resurrection, because the retry path depends on it.
	let store = StandardMultiStore::testing_memory();
	let k = table_row_key(4, 1);

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"v1".to_vec())),
		}],
		CommitVersion(1),
	)
	.unwrap();
	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k.clone())], CommitVersion(4)).unwrap();
	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k.clone())], CommitVersion(6)).unwrap();

	assert_eq!(get(&store, &k, 10), None, "the key stays removed after a second removal");
	assert_eq!(get(&store, &k, 5), None, "and between the two tombstones as well");
	assert_eq!(get(&store, &k, 1).as_deref(), Some(b"v1".as_slice()), "the pre-removal snapshot is untouched");
}
