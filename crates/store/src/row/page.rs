// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	interface::store::EntryKind,
	key::{EncodableKey, Key, row::RowKey},
};
use reifydb_value::value::row_number::RowNumber;

pub const DEFAULT_BUCKET_SHIFT: u8 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
	pub kind: EntryKind,
	pub bucket: u64,
	pub series: bool,
}

pub fn page_of(key: &EncodedKey, bucket_shift: u8) -> PageId {
	match Key::decode(key) {
		Some(Key::Row(row_key)) => PageId {
			kind: EntryKind::Source(row_key.storage),
			bucket: row_key.row.0 >> bucket_shift,
			series: false,
		},
		Some(Key::SeriesRow(series_key)) => PageId {
			kind: EntryKind::Source(series_key.storage),
			bucket: 0,
			series: true,
		},
		Some(Key::PartitionedRow(partitioned_key)) => PageId {
			kind: EntryKind::PartitionedSource(partitioned_key.storage),
			bucket: 0,
			series: false,
		},
		Some(Key::PartitionedSeriesRow(partitioned_key)) => PageId {
			kind: EntryKind::PartitionedSource(partitioned_key.storage),
			bucket: 0,
			series: true,
		},
		_ => PageId {
			kind: EntryKind::Multi,
			bucket: 0,
			series: false,
		},
	}
}

pub fn key_range_of(page: PageId, bucket_shift: u8) -> Option<EncodedKeyRange> {
	if page.series {
		return None;
	}

	match page.kind {
		EntryKind::Source(storage) => {
			let low = page.bucket << bucket_shift;
			let high = low | ((1u64 << bucket_shift) - 1);
			let start = RowKey {
				storage,
				row: RowNumber(high),
			}
			.encode();
			let end = RowKey {
				storage,
				row: RowNumber(low),
			}
			.encode();
			Some(EncodedKeyRange::new(Bound::Included(start), Bound::Included(end)))
		}
		EntryKind::PartitionedSource(_) => None,
		EntryKind::Multi => None,
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		interface::{
			catalog::{
				id::{SeriesId, ViewId},
				storage::StorageId,
			},
			store::{EntryKind, classify_key},
		},
		key::{
			EncodableKey, partitioned_row::PartitionedRowKey,
			partitioned_series_row::PartitionedSeriesRowKey, row::RowKey, series_row::SeriesRowKey,
		},
	};
	use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

	use super::{key_range_of, page_of};

	fn row(storage: StorageId, n: u64) -> EncodedKey {
		RowKey {
			storage,
			row: RowNumber(n),
		}
		.encode()
	}

	#[test]
	fn page_of_partitioned_row_is_partitioned_source_with_no_key_range() {
		let storage = StorageId::table(7);
		let key = PartitionedRowKey::encoded(
			storage,
			Partition::of(&[Value::Utf8("us".to_string())]),
			RowNumber(100),
		);
		let page = page_of(&key, 16);
		assert_eq!(page.kind, EntryKind::PartitionedSource(storage));
		assert_eq!(page.bucket, 0, "partitioned pages use bucket 0 (key_range_of returns None)");
		assert!(
			key_range_of(page, 16).is_none(),
			"a partitioned page has no reconstructable key range (partition is not in PageId)"
		);
	}

	#[test]
	fn page_of_is_pure_and_buckets_by_row_number() {
		let storage = StorageId::table(7);
		let a = page_of(&row(storage, 100), 16);
		assert_eq!(a, page_of(&row(storage, 100), 16), "page_of must be a pure function of the key");
		assert_eq!(a.kind, EntryKind::Source(storage));
		assert_eq!(a.bucket, 0);

		// 200 is in the same bucket as 100 at shift 16; 1<<16 starts the next bucket.
		assert_eq!(a, page_of(&row(storage, 200), 16));
		assert_eq!(page_of(&row(storage, 1 << 16), 16).bucket, 1);
		assert_ne!(a, page_of(&row(storage, 1 << 16), 16));
	}

	#[test]
	fn page_of_survives_inline_vs_heap_representation() {
		let storage = StorageId::table(3);
		let encoded = row(storage, 42);
		let heap = EncodedKey::new(encoded.as_slice());
		assert_eq!(page_of(&encoded, 16), page_of(&heap, 16));
	}

	#[test]
	fn page_of_distinguishes_source_from_unknown() {
		let storage = StorageId::table(1);
		assert!(matches!(page_of(&row(storage, 0), 16).kind, EntryKind::Source(_)));
		assert_eq!(page_of(&EncodedKey::new(vec![0u8; 8]), 16).kind, EntryKind::Multi);
	}

	#[test]
	fn key_range_of_contains_exactly_its_bucket() {
		let storage = StorageId::table(3);
		let shift = 4u8;

		// bucket 2 at shift 4 covers row numbers [32, 47].
		let page = page_of(&row(storage, 40), shift);
		assert_eq!(page.bucket, 2);

		let range = key_range_of(page, shift).expect("Source pages have a key range");

		assert!(range.contains(&row(storage, 32)), "low boundary row must be in range");
		assert!(range.contains(&row(storage, 47)), "high boundary row must be in range");
		assert!(!range.contains(&row(storage, 31)), "row below the bucket must be excluded");
		assert!(!range.contains(&row(storage, 48)), "row above the bucket must be excluded");
	}

	#[test]
	fn key_range_of_is_none_for_non_source() {
		let unknown = page_of(&EncodedKey::new(vec![0u8; 8]), 16);
		assert!(key_range_of(unknown, 16).is_none());
	}

	#[test]
	fn page_of_series_row_buckets_by_series_never_by_the_series_key() {
		// Bucketing a series row by its key would shard one series across pages by timestamp.
		let early = SeriesRowKey {
			storage: StorageId::series(SeriesId(7)),
			variant_tag: None,
			key: 1,
			sequence: 1,
		}
		.encode();
		let late = SeriesRowKey {
			storage: StorageId::series(SeriesId(7)),
			variant_tag: None,
			key: 1 << 40,
			sequence: 2,
		}
		.encode();

		let page = page_of(&early, 16);
		assert_eq!(page.kind, EntryKind::Source(StorageId::series(SeriesId(7))));
		assert_eq!(page.bucket, 0);
		assert_eq!(page, page_of(&late, 16), "two keys of one series must share a page whatever their key");
	}

	#[test]
	fn page_of_series_row_agrees_with_classify_key() {
		// The page cache and the physical tier must name the same entry, or a cached page shadows another
		// table.
		let key = SeriesRowKey {
			storage: StorageId::series(SeriesId(7)),
			variant_tag: Some(2),
			key: 99,
			sequence: 3,
		}
		.encode();
		assert_eq!(page_of(&key, 16).kind, classify_key(&key));
	}

	#[test]
	fn key_range_of_is_none_for_a_series_page() {
		// A series page carries neither its variant tag nor a key span, so no exact range is reconstructable.
		let key = SeriesRowKey {
			storage: StorageId::series(SeriesId(7)),
			variant_tag: None,
			key: 500,
			sequence: 1,
		}
		.encode();
		assert!(key_range_of(page_of(&key, 16), 16).is_none());
	}

	#[test]
	fn key_range_of_is_none_for_a_series_keyed_view_page() {
		// A series-backed view stores series keys under a View storage id, so its page reaches the generic
		// Source arm; handing back a RowKey bucket range there would mark the page complete over a range no
		// series key can ever fall in, and every read of that view would then be served an empty page.
		let key = SeriesRowKey {
			storage: StorageId::View(ViewId(7)),
			variant_tag: None,
			key: 500,
			sequence: 1,
		}
		.encode();
		let page = page_of(&key, 16);
		assert_eq!(page.kind, EntryKind::Source(StorageId::View(ViewId(7))));
		assert_eq!(page.kind, classify_key(&key));
		assert!(key_range_of(page, 16).is_none(), "a series-keyed view page has no row-number range");
	}

	#[test]
	fn page_of_partitioned_series_row_agrees_with_classify_key() {
		// The page cache and the physical tier must name the same entry for the new partitioned series kind,
		// or a cached page shadows a different object.
		let storage = StorageId::series(SeriesId(4));
		let key = PartitionedSeriesRowKey::encoded(
			storage,
			Partition::of(&[Value::Utf8("us".to_string())]),
			Some(2),
			99,
			3,
		);
		let page = page_of(&key, 16);
		assert_eq!(page.kind, EntryKind::PartitionedSource(storage));
		assert_eq!(page.bucket, 0);
		assert_eq!(page.kind, classify_key(&key));
		assert!(key_range_of(page, 16).is_none());
	}

	#[test]
	fn key_range_of_survives_for_a_table_keyed_view_page() {
		// A view page is only range-less because of series keys, not because it is a view: blanket-denying
		// every view page would silently drop the range-cache hit for every table-backed view in the system.
		let key = RowKey {
			storage: StorageId::View(ViewId(7)),
			row: RowNumber(40),
		}
		.encode();
		let page = page_of(&key, 16);
		assert!(!page.series, "a plain row key must never be marked as a series page");
		let range = key_range_of(page, 16).expect("a table-keyed view page must keep its bucket range");
		assert!(range.contains(&key), "the reconstructed range must contain the key it came from");
	}
}
