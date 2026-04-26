// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! File-backed `SqliteCdcStorage` persistence: write entries, drop, reopen
//! at the same path, verify everything survives.

use std::collections::Bound;

use reifydb_cdc::storage::{CdcStorage, sqlite::storage::SqliteCdcStorage};
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_testing::tempdir::temp_dir;
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
fn persistence_across_reopen() {
	temp_dir(|path| {
		let cfg = SqliteConfig::new(path.join("cdc.reifydb"));

		{
			let store = SqliteCdcStorage::new(cfg.clone());
			for v in 1..=100u64 {
				store.write(&cdc_at(v)).unwrap();
			}
			assert_eq!(store.max_version().unwrap(), Some(CommitVersion(100)));
			store.shutdown();
		}

		let store = SqliteCdcStorage::new(cfg);
		assert_eq!(store.max_version().unwrap(), Some(CommitVersion(100)));
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(1)));
		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 256).unwrap();
		assert_eq!(batch.items.len(), 100);
		assert!(!batch.has_more);
		assert_eq!(batch.items[0].version, CommitVersion(1));
		assert_eq!(batch.items[99].version, CommitVersion(100));
		Ok(())
	})
	.unwrap();
}
