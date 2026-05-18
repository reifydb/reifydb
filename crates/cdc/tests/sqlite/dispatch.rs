// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::Bound;

use reifydb_cdc::storage::CdcStore;
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_type::{util::cowvec::CowVec, value::datetime::DateTime};

fn cdc_at(version: u64) -> Cdc {
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

#[test]
fn dispatch_through_cdcstore_enum() {
	let store = CdcStore::sqlite(SqliteConfig::test());
	store.write(&cdc_at(1)).unwrap();
	store.write(&cdc_at(2)).unwrap();
	store.write(&cdc_at(3)).unwrap();

	assert!(store.read(CommitVersion(2)).unwrap().is_some());
	let batch =
		store.read_range(Bound::Included(CommitVersion(1)), Bound::Included(CommitVersion(3)), 100).unwrap();
	assert_eq!(batch.items.len(), 3);
	assert_eq!(store.count(CommitVersion(1)).unwrap(), 1);
	assert_eq!(store.min_version().unwrap(), Some(CommitVersion(1)));
	assert_eq!(store.max_version().unwrap(), Some(CommitVersion(3)));

	let dropped = store.delete_before(CommitVersion(3)).unwrap();
	assert_eq!(dropped.count, 2);
	assert_eq!(store.min_version().unwrap(), Some(CommitVersion(3)));

	let _ = store.find_ttl_cutoff(DateTime::from_nanos(0)).unwrap();
}
