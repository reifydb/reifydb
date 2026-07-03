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
}

pub fn page_of(key: &EncodedKey, bucket_shift: u8) -> PageId {
	match Key::decode(key) {
		Some(Key::Row(row_key)) => PageId {
			kind: EntryKind::Source(row_key.shape),
			bucket: row_key.row.0 >> bucket_shift,
		},
		Some(Key::FlowNodeState(state_key)) => PageId {
			kind: EntryKind::Operator(state_key.node),
			bucket: 0,
		},
		Some(Key::FlowNodeInternalState(internal_key)) => PageId {
			kind: EntryKind::Operator(internal_key.node),
			bucket: 0,
		},
		_ => PageId {
			kind: EntryKind::Multi,
			bucket: 0,
		},
	}
}

pub fn key_range_of(page: PageId, bucket_shift: u8) -> Option<EncodedKeyRange> {
	match page.kind {
		EntryKind::Source(shape) => {
			let low = page.bucket << bucket_shift;
			let high = low | ((1u64 << bucket_shift) - 1);
			let start = RowKey {
				shape,
				row: RowNumber(high),
			}
			.encode();
			let end = RowKey {
				shape,
				row: RowNumber(low),
			}
			.encode();
			Some(EncodedKeyRange::new(Bound::Included(start), Bound::Included(end)))
		}
		EntryKind::Operator(_) | EntryKind::Multi => None,
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		interface::{catalog::shape::ShapeId, store::EntryKind},
		key::{EncodableKey, row::RowKey},
	};
	use reifydb_value::value::row_number::RowNumber;

	use super::{key_range_of, page_of};

	fn row(shape: ShapeId, n: u64) -> EncodedKey {
		RowKey {
			shape,
			row: RowNumber(n),
		}
		.encode()
	}

	#[test]
	fn page_of_is_pure_and_buckets_by_row_number() {
		let shape = ShapeId::table(7);
		let a = page_of(&row(shape, 100), 16);
		assert_eq!(a, page_of(&row(shape, 100), 16), "page_of must be a pure function of the key");
		assert_eq!(a.kind, EntryKind::Source(shape));
		assert_eq!(a.bucket, 0);

		// 200 is in the same bucket as 100 at shift 16; 1<<16 starts the next bucket.
		assert_eq!(a, page_of(&row(shape, 200), 16));
		assert_eq!(page_of(&row(shape, 1 << 16), 16).bucket, 1);
		assert_ne!(a, page_of(&row(shape, 1 << 16), 16));
	}

	#[test]
	fn page_of_survives_inline_vs_heap_representation() {
		let shape = ShapeId::table(3);
		let encoded = row(shape, 42);
		let heap = EncodedKey::new(encoded.as_slice().to_vec());
		assert_eq!(page_of(&encoded, 16), page_of(&heap, 16));
	}

	#[test]
	fn page_of_distinguishes_source_from_unknown() {
		let shape = ShapeId::table(1);
		assert!(matches!(page_of(&row(shape, 0), 16).kind, EntryKind::Source(_)));
		assert_eq!(page_of(&EncodedKey::new(vec![0u8; 8]), 16).kind, EntryKind::Multi);
	}

	#[test]
	fn key_range_of_contains_exactly_its_bucket() {
		let shape = ShapeId::table(3);
		let shift = 4u8;

		// bucket 2 at shift 4 covers row numbers [32, 47].
		let page = page_of(&row(shape, 40), shift);
		assert_eq!(page.bucket, 2);

		let range = key_range_of(page, shift).expect("Source pages have a key range");

		assert!(range.contains(&row(shape, 32)), "low boundary row must be in range");
		assert!(range.contains(&row(shape, 47)), "high boundary row must be in range");
		assert!(!range.contains(&row(shape, 31)), "row below the bucket must be excluded");
		assert!(!range.contains(&row(shape, 48)), "row above the bucket must be excluded");
	}

	#[test]
	fn key_range_of_is_none_for_non_source() {
		let unknown = page_of(&EncodedKey::new(vec![0u8; 8]), 16);
		assert!(key_range_of(unknown, 16).is_none());
	}
}
