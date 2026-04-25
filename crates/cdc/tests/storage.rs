// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Trait-level storage tests, parametrized across `MemoryCdcStorage` and
//! `SqliteCdcStorage`. Every assertion is wired into both backends via the
//! `storage_trait_tests!` macro. Backend-specific scenarios live in
//! `tests/memory.rs` and `tests/sqlite.rs`.

use std::collections::Bound;

use reifydb_cdc::storage::{CdcStorage, DropBeforeResult, memory::MemoryCdcStorage, sqlite::storage::SqliteCdcStorage};
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_type::{util::cowvec::CowVec, value::datetime::DateTime};

fn cdc_minimal(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![1, 2, 3]),
			post: EncodedRow(CowVec::new(vec![10, 20, 30])),
		}],
	)
}

fn assert_write_read_round_trip<S: CdcStorage>(storage: S) {
	let cdc = cdc_minimal(1);
	storage.write(&cdc).unwrap();
	let read = storage.read(CommitVersion(1)).unwrap().expect("entry should exist");
	assert_eq!(read.version, CommitVersion(1));
	assert_eq!(read.system_changes.len(), 1);
}

fn assert_read_nonexistent<S: CdcStorage>(storage: S) {
	assert!(storage.read(CommitVersion(999)).unwrap().is_none());
}

fn assert_range_inclusive<S: CdcStorage>(storage: S) {
	for v in 1..=10 {
		storage.write(&cdc_minimal(v)).unwrap();
	}
	let batch =
		storage.read_range(Bound::Included(CommitVersion(3)), Bound::Included(CommitVersion(7)), 100).unwrap();
	assert_eq!(batch.items.len(), 5);
	assert!(!batch.has_more);
	assert_eq!(batch.items[0].version, CommitVersion(3));
	assert_eq!(batch.items[4].version, CommitVersion(7));
}

fn assert_range_exclusive<S: CdcStorage>(storage: S) {
	for v in 1..=5 {
		storage.write(&cdc_minimal(v)).unwrap();
	}
	let batch =
		storage.read_range(Bound::Excluded(CommitVersion(2)), Bound::Included(CommitVersion(4)), 100).unwrap();
	assert_eq!(batch.items.len(), 2);
	assert_eq!(batch.items[0].version, CommitVersion(3));
	assert_eq!(batch.items[1].version, CommitVersion(4));

	let batch =
		storage.read_range(Bound::Included(CommitVersion(2)), Bound::Excluded(CommitVersion(4)), 100).unwrap();
	assert_eq!(batch.items.len(), 2);
	assert_eq!(batch.items[0].version, CommitVersion(2));
	assert_eq!(batch.items[1].version, CommitVersion(3));
}

fn assert_range_batch_size_has_more<S: CdcStorage>(storage: S) {
	for v in 1..=10 {
		storage.write(&cdc_minimal(v)).unwrap();
	}
	let batch = storage.read_range(Bound::Unbounded, Bound::Unbounded, 3).unwrap();
	assert_eq!(batch.items.len(), 3);
	assert!(batch.has_more);
}

fn assert_count<S: CdcStorage>(storage: S) {
	let cdc = Cdc::new(
		CommitVersion(1),
		DateTime::from_nanos(1),
		Vec::new(),
		(0..5).map(|i| SystemChange::Insert {
			key: EncodedKey::new(vec![i as u8]),
			post: EncodedRow(CowVec::new(vec![])),
		})
		.collect(),
	);
	storage.write(&cdc).unwrap();
	assert_eq!(storage.count(CommitVersion(1)).unwrap(), 5);
	assert_eq!(storage.count(CommitVersion(2)).unwrap(), 0);
}

fn assert_min_max_version<S: CdcStorage>(storage: S) {
	assert!(storage.min_version().unwrap().is_none());
	assert!(storage.max_version().unwrap().is_none());

	storage.write(&cdc_minimal(5)).unwrap();
	storage.write(&cdc_minimal(3)).unwrap();
	storage.write(&cdc_minimal(8)).unwrap();

	assert_eq!(storage.min_version().unwrap(), Some(CommitVersion(3)));
	assert_eq!(storage.max_version().unwrap(), Some(CommitVersion(8)));
}

fn assert_overwrite<S: CdcStorage>(storage: S) {
	let cdc1 = Cdc::new(
		CommitVersion(1),
		DateTime::from_nanos(100),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![1]),
			post: EncodedRow(CowVec::new(vec![])),
		}],
	);
	let cdc2 = Cdc::new(
		CommitVersion(1),
		DateTime::from_nanos(200),
		Vec::new(),
		vec![
			SystemChange::Insert {
				key: EncodedKey::new(vec![2]),
				post: EncodedRow(CowVec::new(vec![])),
			},
			SystemChange::Insert {
				key: EncodedKey::new(vec![3]),
				post: EncodedRow(CowVec::new(vec![])),
			},
		],
	);
	storage.write(&cdc1).unwrap();
	assert_eq!(storage.count(CommitVersion(1)).unwrap(), 1);
	storage.write(&cdc2).unwrap();
	assert_eq!(storage.count(CommitVersion(1)).unwrap(), 2);
	let read = storage.read(CommitVersion(1)).unwrap().unwrap();
	assert_eq!(read.timestamp, DateTime::from_nanos(200));
}

fn assert_drop_before_empty<S: CdcStorage>(storage: S) {
	let r = storage.drop_before(CommitVersion(10)).unwrap();
	assert_eq!(r.count, 0);
	assert!(r.entries.is_empty());
}

fn assert_drop_before_some<S: CdcStorage>(storage: S) {
	for v in [1u64, 3, 5, 7, 9] {
		storage.write(&cdc_minimal(v)).unwrap();
	}
	let r = storage.drop_before(CommitVersion(5)).unwrap();
	assert_eq!(r.count, 2);
	assert_eq!(r.entries.len(), 2);
	assert!(storage.read(CommitVersion(1)).unwrap().is_none());
	assert!(storage.read(CommitVersion(3)).unwrap().is_none());
	assert!(storage.read(CommitVersion(5)).unwrap().is_some());
	assert_eq!(storage.min_version().unwrap(), Some(CommitVersion(5)));
}

fn assert_drop_before_all<S: CdcStorage>(storage: S) {
	for v in 1..=3u64 {
		storage.write(&cdc_minimal(v)).unwrap();
	}
	let r = storage.drop_before(CommitVersion(10)).unwrap();
	assert_eq!(r.count, 3);
	assert!(storage.min_version().unwrap().is_none());
}

fn assert_drop_before_none_when_too_low<S: CdcStorage>(storage: S) {
	for v in 5..=7u64 {
		storage.write(&cdc_minimal(v)).unwrap();
	}
	let r = storage.drop_before(CommitVersion(3)).unwrap();
	assert_eq!(r.count, 0);
	assert!(r.entries.is_empty());
	assert_eq!(storage.min_version().unwrap(), Some(CommitVersion(5)));
}

fn assert_drop_before_boundary<S: CdcStorage>(storage: S) {
	for v in 1..=5u64 {
		storage.write(&cdc_minimal(v)).unwrap();
	}
	let r = storage.drop_before(CommitVersion(3)).unwrap();
	assert_eq!(r.count, 2);
	assert!(storage.read(CommitVersion(3)).unwrap().is_some());
	assert_eq!(storage.min_version().unwrap(), Some(CommitVersion(3)));
}

fn assert_drop_before_entry_stats<S: CdcStorage>(storage: S) {
	let cdc = Cdc::new(
		CommitVersion(1),
		DateTime::from_nanos(12345),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![1, 2, 3]),
			post: EncodedRow(CowVec::new(vec![10, 20, 30, 40, 50])),
		}],
	);
	storage.write(&cdc).unwrap();
	let r: DropBeforeResult = storage.drop_before(CommitVersion(2)).unwrap();
	assert_eq!(r.count, 1);
	assert_eq!(r.entries.len(), 1);
	assert_eq!(r.entries[0].key.as_ref(), &[1, 2, 3]);
	assert_eq!(r.entries[0].value_bytes, 5);
}

macro_rules! storage_trait_tests {
	($mod_name:ident, $fresh:expr) => {
		mod $mod_name {
			use super::*;
			#[test]
			fn write_read_round_trip() {
				super::assert_write_read_round_trip($fresh);
			}
			#[test]
			fn read_nonexistent() {
				super::assert_read_nonexistent($fresh);
			}
			#[test]
			fn range_inclusive() {
				super::assert_range_inclusive($fresh);
			}
			#[test]
			fn range_exclusive() {
				super::assert_range_exclusive($fresh);
			}
			#[test]
			fn range_batch_size_has_more() {
				super::assert_range_batch_size_has_more($fresh);
			}
			#[test]
			fn count() {
				super::assert_count($fresh);
			}
			#[test]
			fn min_max_version() {
				super::assert_min_max_version($fresh);
			}
			#[test]
			fn overwrite_entry() {
				super::assert_overwrite($fresh);
			}
			#[test]
			fn drop_before_empty() {
				super::assert_drop_before_empty($fresh);
			}
			#[test]
			fn drop_before_some() {
				super::assert_drop_before_some($fresh);
			}
			#[test]
			fn drop_before_all() {
				super::assert_drop_before_all($fresh);
			}
			#[test]
			fn drop_before_none_when_too_low() {
				super::assert_drop_before_none_when_too_low($fresh);
			}
			#[test]
			fn drop_before_boundary() {
				super::assert_drop_before_boundary($fresh);
			}
			#[test]
			fn drop_before_entry_stats() {
				super::assert_drop_before_entry_stats($fresh);
			}
		}
	};
}

storage_trait_tests!(memory, MemoryCdcStorage::new());
storage_trait_tests!(sqlite, SqliteCdcStorage::new(SqliteConfig::test()));
