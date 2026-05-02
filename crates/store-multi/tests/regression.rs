// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::EntryKind,
	},
};
use reifydb_store_multi::{
	hot::storage::HotStorage,
	tier::{RangeBatch, RangeCursor, TierStorage},
};
use reifydb_type::util::cowvec::CowVec;

fn k(s: &str) -> CowVec<u8> {
	CowVec::new(s.as_bytes().to_vec())
}

fn v(s: &str) -> CowVec<u8> {
	CowVec::new(s.as_bytes().to_vec())
}

fn shape() -> EntryKind {
	EntryKind::Source(ShapeId::Table(TableId(2024)))
}

fn drain_forward(s: &HotStorage, kind: EntryKind, version: CommitVersion, batch_size: usize) -> Vec<Vec<u8>> {
	let mut cursor = RangeCursor::new();
	let mut out = Vec::new();
	loop {
		let RangeBatch {
			entries,
			has_more,
		} = s.range_next(kind, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, batch_size).unwrap();
		for e in entries {
			out.push(e.key.as_slice().to_vec());
		}
		if !has_more || cursor.is_exhausted() {
			break;
		}
	}
	out
}

#[test]
fn paginated_range_does_not_truncate_when_filtered_key_is_inside_limit_window() {
	for storage in [HotStorage::memory(), HotStorage::sqlite_in_memory()] {
		let kind = shape();

		for key in ["a", "b", "d", "e", "f", "g", "h", "i"] {
			storage.set(CommitVersion(1), HashMap::from([(kind, vec![(k(key), Some(v("v1")))])])).unwrap();
		}

		storage.set(CommitVersion(10), HashMap::from([(kind, vec![(k("c"), Some(v("v10")))])])).unwrap();

		let result = drain_forward(&storage, kind, CommitVersion(3), 7);

		let recovered: Vec<&str> = result.iter().map(|kk| std::str::from_utf8(kk).unwrap()).collect();
		assert_eq!(
			recovered,
			vec!["a", "b", "d", "e", "f", "g", "h", "i"],
			"expected 8 keys; c is in __current at v=10 with no v<=3 history (filtered out at this snapshot), but the range scan must still page past it to reach h and i past the SQL LIMIT window"
		);
	}
}

#[test]
fn paginated_range_includes_trailing_tombstone_after_filter_skip() {
	for storage in [HotStorage::memory(), HotStorage::sqlite_in_memory()] {
		let kind = shape();

		for key in ["a", "b", "c", "d", "e", "f", "g", "h"] {
			storage.set(CommitVersion(1), HashMap::from([(kind, vec![(k(key), Some(v("init")))])]))
				.unwrap();
		}

		storage.set(CommitVersion(2), HashMap::from([(kind, vec![(k("z"), None)])])).unwrap();

		storage.set(CommitVersion(8), HashMap::from([(kind, vec![(k("a"), Some(v("v8")))])])).unwrap();

		let result = drain_forward(&storage, kind, CommitVersion(3), 7);

		let z_visible = result.iter().any(|kk| kk == b"z");
		assert!(z_visible, "tombstone z at v=2 must appear in v=3 paginated range; got {:?}", result);
	}
}
