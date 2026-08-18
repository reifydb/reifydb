// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		bytes::{EncodedBytes, read_fingerprint},
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
};
use reifydb_core::{
	error::diagnostic::internal::internal,
	interface::{
		catalog::object::ObjectId,
		cdc::{Cdc, SystemChange},
		change::{Change, ChangeOrigin, Diff, Diffs},
	},
	key::{Key, partitioned_row::RowLocator},
	value::column::columns::Columns,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error, value::row_number::RowNumber};

pub struct RowTarget {
	pub object: ObjectId,
	pub row: RowNumber,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RebuiltKind {
	Insert,
	Update,
	Remove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BucketKey {
	kind: RebuiltKind,
	post_shape: RowShapeFingerprint,
	pre_shape: RowShapeFingerprint,
}

#[derive(Default)]
struct Bucket {
	ids: Vec<RowNumber>,
	pre: Vec<EncodedBytes>,
	post: Vec<EncodedBytes>,
}

pub fn row_target(key: &EncodedKey) -> Option<RowTarget> {
	match Key::decode(key)? {
		Key::Row(row_key) => Some(RowTarget {
			object: ObjectId::from(row_key.storage),
			row: row_key.row,
		}),
		Key::SeriesRow(series_key) => Some(RowTarget {
			object: ObjectId::series(series_key.series),
			row: RowNumber(series_key.sequence),
		}),
		Key::PartitionedRow(partitioned) => Some(RowTarget {
			object: ObjectId::from(partitioned.storage),
			row: match partitioned.locator {
				RowLocator::Row(row) => row,
				RowLocator::Series {
					sequence,
					..
				} => RowNumber(sequence),
			},
		}),
		_ => None,
	}
}

pub fn rebuild_changes(cdc: &Cdc, catalog: &Catalog, txn: &mut Transaction<'_>) -> Result<Vec<Change>> {
	let mut grouped: BTreeMap<ObjectId, BTreeMap<BucketKey, Bucket>> = BTreeMap::new();

	for system_change in &cdc.system_changes {
		let Some(target) = row_target(system_change.key()) else {
			continue;
		};
		if matches!(target.object, ObjectId::Queue(_)) {
			continue;
		}
		let (key, pre, post) = match system_change {
			SystemChange::Insert {
				post,
				..
			} => {
				let fingerprint = read_fingerprint(post);
				(
					BucketKey {
						kind: RebuiltKind::Insert,
						post_shape: fingerprint,
						pre_shape: fingerprint,
					},
					None,
					Some(post.clone()),
				)
			}
			SystemChange::Update {
				pre,
				post,
				..
			} => (
				BucketKey {
					kind: RebuiltKind::Update,
					post_shape: read_fingerprint(post),
					pre_shape: read_fingerprint(pre),
				},
				Some(pre.clone()),
				Some(post.clone()),
			),
			SystemChange::Delete {
				key,
				pre,
			} => {
				let pre = pre.as_ref().ok_or_else(|| {
					Error(Box::new(internal(format!(
						"CDC delete for key {:?} at version {} carries no pre-image, so its \
						 change cannot be rebuilt",
						key.as_slice(),
						cdc.version.0
					))))
				})?;
				let fingerprint = read_fingerprint(pre);
				(
					BucketKey {
						kind: RebuiltKind::Remove,
						post_shape: fingerprint,
						pre_shape: fingerprint,
					},
					Some(pre.clone()),
					None,
				)
			}
		};

		let bucket = grouped.entry(target.object).or_default().entry(key).or_default();
		bucket.ids.push(target.row);
		if let Some(pre) = pre {
			bucket.pre.push(pre);
		}
		if let Some(post) = post {
			bucket.post.push(post);
		}
	}

	let mut shapes: BTreeMap<RowShapeFingerprint, RowShape> = BTreeMap::new();
	let mut changes: Vec<Change> = Vec::with_capacity(grouped.len());

	for (object, buckets) in grouped {
		let mut diffs: Diffs = Diffs::new();
		for (key, bucket) in buckets {
			let diff = match key.kind {
				RebuiltKind::Insert => {
					let shape = load_shape(catalog, txn, &mut shapes, key.post_shape)?;
					Diff::insert(Columns::from_encoded_bytes(&shape, &bucket.ids, &bucket.post))
				}
				RebuiltKind::Update => {
					let pre_shape = load_shape(catalog, txn, &mut shapes, key.pre_shape)?;
					let post_shape = load_shape(catalog, txn, &mut shapes, key.post_shape)?;
					Diff::update(
						Columns::from_encoded_bytes(&pre_shape, &bucket.ids, &bucket.pre),
						Columns::from_encoded_bytes(&post_shape, &bucket.ids, &bucket.post),
					)
				}
				RebuiltKind::Remove => {
					let shape = load_shape(catalog, txn, &mut shapes, key.pre_shape)?;
					Diff::remove(Columns::from_encoded_bytes(&shape, &bucket.ids, &bucket.pre))
				}
			};
			diffs.push(diff);
		}
		changes.push(Change {
			origin: ChangeOrigin::Object(object),
			diffs,
			version: cdc.version,
			changed_at: cdc.timestamp,
		});
	}

	Ok(changes)
}

fn load_shape(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	cache: &mut BTreeMap<RowShapeFingerprint, RowShape>,
	fingerprint: RowShapeFingerprint,
) -> Result<RowShape> {
	if let Some(shape) = cache.get(&fingerprint) {
		return Ok(shape.clone());
	}
	let shape = catalog.get_or_load_row_shape(fingerprint, txn)?.ok_or_else(|| {
		Error(Box::new(internal(format!(
			"RowShape with fingerprint {:?} not found while rebuilding CDC changes",
			fingerprint
		))))
	})?;
	cache.insert(fingerprint, shape.clone());
	Ok(shape)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::catalog::{
			id::{SeriesId, TableId, ViewId},
			storage::StorageId,
		},
		key::{EncodableKey, partitioned_row::PartitionedRowKey, row::RowKey, series_row::SeriesRowKey},
	};
	use reifydb_value::value::partition::Partition;

	use super::*;

	#[test]
	fn test_row_key_maps_to_its_storage_object() {
		let target = row_target(&RowKey::encoded(StorageId::table(3), RowNumber(9))).expect("row target");
		assert_eq!(target.object, ObjectId::Table(TableId(3)));
		assert_eq!(target.row, RowNumber(9));
	}

	#[test]
	fn test_view_row_key_maps_to_the_view_and_never_to_its_former_backing_table() {
		let target = row_target(&RowKey::encoded(StorageId::view(42), RowNumber(1))).expect("row target");
		assert_eq!(target.object, ObjectId::View(ViewId(42)));
	}

	#[test]
	fn test_partitioned_row_key_maps_to_its_storage_object() {
		let key = PartitionedRowKey::encoded(StorageId::view(8), Partition(5), RowLocator::Row(RowNumber(2)));
		let target = row_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::View(ViewId(8)));
		assert_eq!(target.row, RowNumber(2));
	}

	#[test]
	fn test_partitioned_series_locator_uses_the_sequence_as_row_number() {
		let key = PartitionedRowKey::encoded(
			StorageId::series(4),
			Partition(1),
			RowLocator::Series {
				variant_tag: None,
				key: 1_000,
				sequence: 7,
			},
		);
		assert_eq!(row_target(&key).expect("row target").row, RowNumber(7));
	}

	#[test]
	fn test_series_row_key_uses_the_sequence_as_row_number_never_the_series_key() {
		// A RowKey decode of the longer series suffix would invent RowNumber(1_000) out of the key bytes.
		let key = SeriesRowKey {
			series: SeriesId(4),
			variant_tag: None,
			key: 1_000,
			sequence: 7,
		}
		.encode();
		assert!(RowKey::decode(&key).is_none(), "a series row key must no longer decode as a plain row key");
		let target = row_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::Series(SeriesId(4)));
		assert_eq!(target.row, RowNumber(7));
	}

	#[test]
	fn test_tagged_series_row_key_maps_to_its_series() {
		// The variant tag shifts the key and sequence by one byte, so the tagged layout needs its own cover.
		let key = SeriesRowKey {
			series: SeriesId(9),
			variant_tag: Some(3),
			key: 1_000,
			sequence: 11,
		}
		.encode();
		let target = row_target(&key).expect("row target");
		assert_eq!(target.object, ObjectId::Series(SeriesId(9)));
		assert_eq!(target.row, RowNumber(11));
	}

	#[test]
	fn test_catalog_keys_are_skipped() {
		assert!(row_target(&EncodedKey::new(b"not a row key".to_vec())).is_none());
	}
}
