// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_cdc::storage::{CdcStorage, sqlite::storage::SqliteCdcStorage};
use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_testing::tempdir::temp_dir;
use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

fn cdc_at(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![1, 2, 3]),
			post: EncodedBytes(CowVec::new(vec![10, 20, 30])),
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

#[test]
fn the_truncation_floor_survives_a_reopen() {
	// Overtaken detection compares consumer cursors against this floor, so a floor that reset to 0
	// on restart would let a consumer below truncated history resume and skip the gap.
	temp_dir(|path| {
		let cfg = SqliteConfig::new(path.join("cdc.reifydb"));

		{
			let store = SqliteCdcStorage::new(cfg.clone());
			for v in 1..=10u64 {
				store.write(&cdc_at(v)).unwrap();
			}
			assert_eq!(store.truncated_before().unwrap(), CommitVersion(0), "no truncation ran yet");
			store.drop_before(CommitVersion(6), usize::MAX).unwrap();
			assert_eq!(store.truncated_before().unwrap(), CommitVersion(6));
			store.shutdown();
		}

		let store = SqliteCdcStorage::new(cfg);
		assert_eq!(
			store.truncated_before().unwrap(),
			CommitVersion(6),
			"the truncation floor must be durable across a reopen"
		);
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(6)), "versions below the floor are gone");
		Ok(())
	})
	.unwrap();
}
