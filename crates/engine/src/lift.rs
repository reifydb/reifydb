// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::size_of,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::{
	bytes::{EncodedBytes, read_fingerprint},
	shape::{RowShape, fingerprint::RowShapeFingerprint},
};
use reifydb_core::{
	common::{ChangeVersion, SourceVersion},
	delta::{Delta, RemoveAnnounce, SetPre},
	interface::{
		catalog::object::ObjectId,
		change::{Change, ChangeOrigin, Diff, Diffs},
		commit::{CommitEntry, CommitLift, CommitObjectLift},
	},
	internal_error,
	key::any::TaggedKey,
	value::batch::from_encoded_bytes,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

#[derive(Clone, Copy)]
pub struct RowTarget {
	pub object: ObjectId,
	pub row: RowNumber,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LiftedKind {
	Remove,
	Update,
	Insert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BucketKey {
	kind: LiftedKind,
	post_shape: RowShapeFingerprint,
	pre_shape: RowShapeFingerprint,
}

struct LiftedRow {
	target: RowTarget,
	pre: Option<EncodedBytes>,
	post: Option<EncodedBytes>,
}

#[derive(Default)]
struct Bucket {
	ids: Vec<RowNumber>,
	pre: Vec<EncodedBytes>,
	post: Vec<EncodedBytes>,
}

pub fn tagged_row_target(key: &TaggedKey) -> Option<RowTarget> {
	let (storage, row) = match key {
		TaggedKey::Row(key) => (key.storage, key.row),
		TaggedKey::SeriesRow(key) => (key.storage, RowNumber(key.sequence)),
		TaggedKey::PartitionedRow(key) => (key.storage, key.row),
		TaggedKey::PartitionedSeriesRow(key) => (key.storage, RowNumber(key.sequence)),
		TaggedKey::SortedViewRow(key) => (key.storage, key.row.0),
		TaggedKey::PartitionedSortedViewRow(key) => (key.storage, key.row.0),
		_ => return None,
	};
	Some(RowTarget {
		object: ObjectId::from(storage),
		row,
	})
}

fn tracked_tagged_target(key: &TaggedKey) -> Option<RowTarget> {
	untracked_queue(tagged_row_target(key)?)
}

fn untracked_queue(target: RowTarget) -> Option<RowTarget> {
	if matches!(target.object, ObjectId::Queue(_)) {
		return None;
	}
	Some(target)
}

fn entry_lift_index(entry: &CommitEntry) -> &CommitLift {
	entry.lift_with(|entry| {
		let mut objects: BTreeMap<ObjectId, CommitObjectLift> = BTreeMap::new();
		for (index, delta) in entry.deltas.iter().enumerate() {
			let Some(target) = tracked_tagged_target(delta.key()) else {
				continue;
			};
			let object = objects.entry(target.object).or_default();
			if !matches!(
				delta,
				Delta::Remove {
					announce: RemoveAnnounce::Unobserved { .. },
					..
				}
			) {
				object.rows.push((index, target.row));
			}
		}
		CommitLift {
			objects,
		}
	})
}

pub fn entry_changed_objects(entry: &CommitEntry) -> &BTreeSet<ObjectId> {
	entry.changed_objects_with(|entry| entry_lift_index(entry).objects.keys().copied().collect())
}

pub fn entry_lifted_objects(entry: &CommitEntry) -> BTreeSet<ObjectId> {
	let index = entry_lift_index(entry);
	for object in index.objects.values() {
		for (row, _) in &object.rows {
			assert_entry_pre(entry, &entry.deltas[*row]);
		}
	}
	index.objects.iter().filter(|(_, object)| !object.rows.is_empty()).map(|(id, _)| *id).collect()
}

fn assert_entry_pre(entry: &CommitEntry, delta: &Delta) {
	match delta {
		Delta::Set {
			key,
			pre: SetPre::Unknown,
			..
		} => panic!(
			"a set at version {} reached the flow lift with an unresolved pre: {:?}",
			entry.version.commit.0, key
		),
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Silent,
		} => panic!(
			"a remove at version {} reached the flow lift without an announced pre: {:?}",
			entry.version.commit.0, key
		),
		_ => {}
	}
}

pub fn lift_entry_changes(
	entry: &CommitEntry,
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	accept: impl Fn(ObjectId) -> bool,
) -> Result<Vec<Change>> {
	let index = entry_lift_index(entry);
	let mut changes: Vec<Change> = Vec::new();
	for (id, object) in index.objects.iter().filter(|(id, _)| accept(**id)) {
		let lifted = match object.changes.get() {
			Some(lifted) => lifted,
			None => {
				let lifted = lift_entry_object(entry, *id, &object.rows, catalog, txn)?;
				let bytes = lifted.iter().map(change_bytes).sum();
				if object.changes.set(lifted).is_ok() {
					entry.charge(bytes);
				}
				object.changes.get().expect("the lifted changes were stored by this or a racing lift")
			}
		};
		changes.extend(lifted.iter().cloned());
	}
	Ok(changes)
}

fn change_bytes(change: &Change) -> u64 {
	let batches: usize = change
		.diffs
		.iter()
		.map(|diff| match diff {
			Diff::Insert {
				post,
				..
			} => post.get_array_memory_size(),
			Diff::Update {
				pre,
				post,
				..
			} => pre.get_array_memory_size() + post.get_array_memory_size(),
			Diff::Remove {
				pre,
				..
			} => pre.get_array_memory_size(),
		})
		.sum();
	(size_of::<Change>() + batches) as u64
}

fn lift_entry_object(
	entry: &CommitEntry,
	object: ObjectId,
	indexed: &[(usize, RowNumber)],
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
) -> Result<Vec<Change>> {
	let mut layers: BTreeMap<SourceVersion, Vec<LiftedRow>> = BTreeMap::new();
	for (index, row) in indexed {
		let (pre, post) = match &entry.deltas[*index] {
			Delta::Set {
				bytes,
				pre: SetPre::Absent,
				..
			} => (None, Some(bytes.clone())),
			Delta::Set {
				bytes,
				pre: SetPre::Present(pre),
				..
			} => (Some(pre.clone()), Some(bytes.clone())),
			Delta::Set {
				key,
				pre: SetPre::Unknown,
				..
			} => panic!(
				"a set at version {} reached the flow lift with an unresolved pre: {:?}",
				entry.version.commit.0, key
			),
			Delta::Remove {
				announce: RemoveAnnounce::Announced {
					pre,
				},
				..
			} => (Some(pre.clone()), None),
			Delta::Remove {
				key,
				..
			} => panic!(
				"a remove at version {} reached the flow lift without an announced pre: {:?}",
				entry.version.commit.0, key
			),
		};
		layers.entry(entry.source_at(*index)).or_default().push(LiftedRow {
			target: RowTarget {
				object,
				row: *row,
			},
			pre,
			post,
		});
	}
	let mut changes = Vec::with_capacity(layers.len());
	for (source, rows) in layers {
		let version = ChangeVersion {
			commit: entry.version.commit,
			source,
		};
		changes.extend(lift_rows(object, rows, version, entry.at, catalog, txn)?);
	}
	Ok(changes)
}

fn lift_rows(
	object: ObjectId,
	mut rows: Vec<LiftedRow>,
	version: ChangeVersion,
	changed_at: DateTime,
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
) -> Result<Option<Change>> {
	pair_moved_rows(&mut rows);

	let mut buckets: BTreeMap<BucketKey, Bucket> = BTreeMap::new();
	for row in rows {
		let key = match (&row.pre, &row.post) {
			(None, Some(post)) => {
				let fingerprint = read_fingerprint(post);
				BucketKey {
					kind: LiftedKind::Insert,
					post_shape: fingerprint,
					pre_shape: fingerprint,
				}
			}
			(Some(pre), Some(post)) => BucketKey {
				kind: LiftedKind::Update,
				post_shape: read_fingerprint(post),
				pre_shape: read_fingerprint(pre),
			},
			(Some(pre), None) => {
				let fingerprint = read_fingerprint(pre);
				BucketKey {
					kind: LiftedKind::Remove,
					post_shape: fingerprint,
					pre_shape: fingerprint,
				}
			}
			(None, None) => continue,
		};

		let bucket = buckets.entry(key).or_default();
		bucket.ids.push(row.target.row);
		if let Some(pre) = row.pre {
			bucket.pre.push(pre);
		}
		if let Some(post) = row.post {
			bucket.post.push(post);
		}
	}

	if buckets.is_empty() {
		return Ok(None);
	}

	let mut diffs: Diffs = Diffs::new();
	for (key, bucket) in buckets {
		let diff = match key.kind {
			LiftedKind::Insert => {
				let shape = load_shape(catalog, txn, key.post_shape)?;
				Diff::insert(from_encoded_bytes(&shape, &bucket.ids, &bucket.post)?)
			}
			LiftedKind::Update => {
				let pre_shape = load_shape(catalog, txn, key.pre_shape)?;
				let post_shape = load_shape(catalog, txn, key.post_shape)?;
				Diff::update(
					from_encoded_bytes(&pre_shape, &bucket.ids, &bucket.pre)?,
					from_encoded_bytes(&post_shape, &bucket.ids, &bucket.post)?,
				)
			}
			LiftedKind::Remove => {
				let shape = load_shape(catalog, txn, key.pre_shape)?;
				Diff::remove(from_encoded_bytes(&shape, &bucket.ids, &bucket.pre)?)
			}
		};
		diffs.push(diff);
	}
	Ok(Some(Change {
		origin: ChangeOrigin::Object(object),
		diffs,
		version,
		changed_at,
	}))
}

fn pair_moved_rows(rows: &mut Vec<LiftedRow>) {
	let mut inserts: BTreeMap<(ObjectId, RowNumber), usize> = BTreeMap::new();
	for (index, row) in rows.iter().enumerate() {
		if row.pre.is_none() {
			inserts.insert((row.target.object, row.target.row), index);
		}
	}
	for index in 0..rows.len() {
		if rows[index].pre.is_none() || rows[index].post.is_some() {
			continue;
		}
		if let Some(insert) = inserts.remove(&(rows[index].target.object, rows[index].target.row)) {
			rows[index].post = rows[insert].post.take();
		}
	}
	rows.retain(|row| row.pre.is_some() || row.post.is_some());
}

fn load_shape(catalog: &Catalog, txn: &mut Transaction<'_>, fingerprint: RowShapeFingerprint) -> Result<RowShape> {
	catalog.get_or_load_row_shape(fingerprint, txn)?.ok_or_else(|| {
		internal_error!("RowShape with fingerprint {:?} not found while lifting commit changes", fingerprint)
	})
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		interface::catalog::{
			id::{SeriesId, TableId, ViewId},
			storage::StorageId,
		},
		key::{
			row::{PartitionedRowKey, RowKey},
			series::{PartitionedSeriesRowKey, SeriesRowKey},
		},
	};
	use reifydb_value::value::partition::Partition;

	use super::*;

	fn decoded_target(key: &EncodedKey) -> Option<RowTarget> {
		tagged_row_target(&TaggedKey::decode(key)?)
	}

	#[test]
	fn test_row_key_maps_to_its_storage_object() {
		let target = decoded_target(&RowKey::encoded(StorageId::table(3), RowNumber(9))).expect("row target");
		assert_eq!(target.object, ObjectId::Table(TableId(3)));
		assert_eq!(target.row, RowNumber(9));
	}

	#[test]
	fn test_view_row_key_maps_to_the_view_and_never_to_its_former_backing_table() {
		let target = decoded_target(&RowKey::encoded(StorageId::view(42), RowNumber(1))).expect("row target");
		assert_eq!(target.object, ObjectId::View(ViewId(42)));
	}

	#[test]
	fn test_partitioned_row_key_maps_to_its_storage_object() {
		let key = PartitionedRowKey::encoded(StorageId::view(8), Partition(5), RowNumber(2));
		let target = decoded_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::View(ViewId(8)));
		assert_eq!(target.row, RowNumber(2));
	}

	#[test]
	fn test_series_row_key_uses_the_sequence_as_row_number_never_the_series_key() {
		// A RowKey decode of the longer series suffix would invent RowNumber(1_000) out of the key bytes.
		let key = SeriesRowKey {
			storage: StorageId::series(4),
			variant_tag: None,
			key: 1_000,
			sequence: 7,
		}
		.encode();
		assert!(RowKey::decode(&key).is_none(), "a series row key must no longer decode as a plain row key");
		let target = decoded_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::Series(SeriesId(4)));
		assert_eq!(target.row, RowNumber(7));
	}

	#[test]
	fn test_tagged_series_row_key_maps_to_its_series() {
		// The variant tag shifts the key and sequence by one byte, so the tagged layout needs its own cover.
		let key = SeriesRowKey {
			storage: StorageId::series(9),
			variant_tag: Some(3),
			key: 1_000,
			sequence: 11,
		}
		.encode();
		let target = decoded_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::Series(SeriesId(9)));
		assert_eq!(target.row, RowNumber(11));
	}

	#[test]
	fn test_series_row_key_on_a_view_storage_maps_to_the_view_never_to_a_series() {
		// A series-backed view writes series keys under a View storage id; reading the object id back as a
		// series would attribute every lifted change of that view to a series that does not exist.
		let key = SeriesRowKey {
			storage: StorageId::view(8),
			variant_tag: None,
			key: 1_000,
			sequence: 7,
		}
		.encode();
		let target = decoded_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::View(ViewId(8)));
		assert_eq!(target.row, RowNumber(7));
	}

	#[test]
	fn test_partitioned_series_row_key_uses_the_sequence_as_row_number() {
		// The partitioned series kind carries its row identity in the sequence, exactly like the unpartitioned
		// one; taking the series key instead would invent RowNumber(1_000) and misjoin every pre-image.
		let key = PartitionedSeriesRowKey::encoded(StorageId::series(4), Partition(1), None, 1_000, 7);
		let target = decoded_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::Series(SeriesId(4)));
		assert_eq!(target.row, RowNumber(7));
	}

	#[test]
	fn test_partitioned_series_row_key_on_a_view_storage_maps_to_the_view() {
		// Same widening as the unpartitioned case: the storage id decides the object, not the key kind.
		let key = PartitionedSeriesRowKey::encoded(StorageId::view(8), Partition(1), Some(2), 5, 3);
		let target = decoded_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::View(ViewId(8)));
		assert_eq!(target.row, RowNumber(3));
	}

	#[test]
	fn test_catalog_keys_are_skipped() {
		assert!(decoded_target(&EncodedKey::new(b"not a row key")).is_none());
	}
}
