// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::Bound, path::PathBuf};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_cdc::{
	config::CdcCommitConfig,
	error::CdcError,
	storage::{CdcStorage, Cutoff},
	store::CdcStore,
	tier::persistent::CdcPersistentTier,
};
use reifydb_testing::tempdir::temp_dir;
use reifydb_value::{byte_size::ByteSize, count::Count, util::cowvec::CowVec, value::datetime::DateTime};

mod common;

const TIMESTAMP: u64 = 1_700_000_000_000_000_000;

const KEY: [u8; 3] = [1, 2, 3];

const POST_BYTES: u64 = 2;

/// The memory arm hands the same tier to a second store, which must behave identically because every restart guarantee
/// below is the store's, not the file's.
enum Tier {
	Memory(CdcPersistentTier),
	Sqlite(PathBuf),
}

impl Tier {
	fn open(&self) -> CdcPersistentTier {
		match self {
			Tier::Memory(tier) => tier.clone(),
			Tier::Sqlite(path) => CdcPersistentTier::sqlite(SqliteConfig::new(path)),
		}
	}

	fn store(&self) -> CdcStore {
		common::custom(self.open(), None, CdcCommitConfig::default(), None).store
	}
}

fn memory_case(body: impl FnOnce(&Tier)) {
	body(&Tier::Memory(CdcPersistentTier::memory()));
}

fn sqlite_case(body: impl FnOnce(&Tier)) {
	temp_dir(|dir| {
		body(&Tier::Sqlite(dir.join("cdc.db")));
		Ok(())
	})
	.expect("temp dir for the cdc file");
}

fn changes_for(version: u64) -> usize {
	1 + (version % 3) as usize
}

fn changes_total(versions: impl IntoIterator<Item = u64>) -> u64 {
	versions.into_iter().map(|v| changes_for(v) as u64).sum()
}

fn cdc_at(version: u64) -> Cdc {
	// every field derives from the version, so a store that returns a neighbouring record must fail on content
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(TIMESTAMP + version),
		(0..changes_for(version))
			.map(|i| CdcChange::Insert {
				key: EncodedKey::new(KEY.to_vec()),
				post: EncodedBytes(CowVec::new(vec![version as u8, i as u8])),
			})
			.collect(),
	)
}

fn write_all(store: &CdcStore, versions: impl IntoIterator<Item = u64>) {
	for v in versions {
		store.write(&cdc_at(v)).unwrap();
	}
}

fn seal_each(store: &CdcStore, versions: impl IntoIterator<Item = u64>) {
	// one flush per record puts each version in its own block; without that retention finds no whole block to drop
	for v in versions {
		store.write(&cdc_at(v)).unwrap();
		assert!(store.flush_pending(), "flush timed out for version {v}");
	}
}

fn assert_record(store: &CdcStore, version: u64) {
	let cdc = store.read(CommitVersion(version)).unwrap().unwrap_or_else(|| panic!("v{version} must survive"));
	assert_eq!(cdc.version, CommitVersion(version));
	assert_eq!(cdc.timestamp.to_nanos(), TIMESTAMP + version, "v{version} lost its timestamp");
	assert_eq!(cdc.changes.len(), changes_for(version), "v{version} lost changes");
	for (i, change) in cdc.changes.iter().enumerate() {
		assert_eq!(change.value_bytes(), POST_BYTES as usize, "v{version} change {i} lost its payload");
	}
	assert_eq!(store.count(CommitVersion(version)).unwrap(), changes_for(version));
}

fn versions_in(store: &CdcStore, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Vec<u64> {
	store.read_range(start, end, 1024).unwrap().items.iter().map(|cdc| cdc.version.0).collect()
}

fn all_versions(store: &CdcStore) -> Vec<u64> {
	versions_in(store, Bound::Unbounded, Bound::Unbounded)
}

fn assert_sealed_blocks_survive_restart(tier: &Tier) {
	// a store reopened over the same tier must answer every read exactly as the store that sealed the blocks did
	let before = tier.store();
	write_all(&before, 1..=5);
	assert!(before.flush_pending());
	write_all(&before, 6..=10);
	assert!(before.flush_pending());

	let expected = all_versions(&before);
	assert_eq!(expected, (1..=10).collect::<Vec<_>>());
	let min = before.min_version().unwrap();
	let max = before.max_version().unwrap();
	let floor = before.truncated_before().unwrap();
	before.shutdown();

	let after = tier.store();
	assert_eq!(all_versions(&after), expected);
	assert_eq!(after.min_version().unwrap(), min);
	assert_eq!(after.max_version().unwrap(), max);
	assert_eq!(after.truncated_before().unwrap(), floor);
	assert_eq!(after.min_version().unwrap(), Some(CommitVersion(1)));
	assert_eq!(after.max_version().unwrap(), Some(CommitVersion(10)));
	for v in 1..=10 {
		assert_record(&after, v);
	}
	assert_eq!(
		versions_in(&after, Bound::Included(CommitVersion(4)), Bound::Included(CommitVersion(7))),
		vec![4, 5, 6, 7]
	);
	assert!(after.read(CommitVersion(11)).unwrap().is_none(), "a version nobody wrote must stay missing");
}

fn assert_sealed_version_rejected_after_restart(tier: &Tier) {
	// the commit tier opens empty, so without a seal floor from disk a restart re-admits an already sealed version
	let before = tier.store();
	write_all(&before, 1..=5);
	assert!(before.flush_pending());
	before.shutdown();

	let after = tier.store();
	for sealed in [1u64, 3, 5] {
		assert!(
			matches!(after.write(&cdc_at(sealed)), Err(CdcError::DuplicateVersion(v)) if v.0 == sealed),
			"v{sealed} is inside a sealed block and must be rejected after a restart"
		);
	}

	assert!(after.write(&cdc_at(6)).is_ok(), "the first free version after the sealed run must be accepted");
	assert_record(&after, 6);
	assert_eq!(after.max_version().unwrap(), Some(CommitVersion(6)));
	assert_eq!(all_versions(&after), (1..=6).collect::<Vec<_>>());

	for v in 1..=5 {
		assert_record(&after, v);
	}
	assert!(
		matches!(after.write(&cdc_at(6)), Err(CdcError::DuplicateVersion(CommitVersion(6)))),
		"the version just written must be rejected a second time"
	);
}

fn assert_truncation_floor_survives_restart(tier: &Tier) {
	// a restart that forgets the floor reports zero, and an overtaken consumer resumes from a prefix that is gone
	let before = tier.store();
	seal_each(&before, 1..=6);
	before.drop_before(Cutoff::Version(CommitVersion(4)), usize::MAX).unwrap();
	assert_eq!(before.truncated_before().unwrap(), CommitVersion(4));
	before.shutdown();

	let after = tier.store();
	assert_eq!(after.truncated_before().unwrap(), CommitVersion(4));
	assert_eq!(after.min_version().unwrap(), Some(CommitVersion(4)));
	for gone in 1..=3 {
		assert!(after.read(CommitVersion(gone)).unwrap().is_none(), "v{gone} was dropped before restart");
	}
	assert_eq!(all_versions(&after), vec![4, 5, 6]);
	assert_eq!(versions_in(&after, Bound::Included(CommitVersion(1)), Bound::Unbounded), vec![4, 5, 6]);
	for v in 4..=6 {
		assert_record(&after, v);
	}
}

fn assert_retention_after_restart(tier: &Tier) {
	// retention on a reopened store must drop exactly the same blocks and report exactly the same eviction totals
	let before = tier.store();
	seal_each(&before, 1..=6);
	before.shutdown();

	let after = tier.store();
	let first = after.drop_before(Cutoff::Version(CommitVersion(5)), 2).unwrap();
	assert_eq!(first.count, Count::new(changes_total([1, 2])));
	assert!(first.more_remaining, "two of the four droppable blocks are still below the cutoff");
	assert_eq!(first.entries.len(), 1, "every record carries the same key and rolls up into one source");
	assert_eq!(first.entries[0].key_bytes, ByteSize::from_bytes(KEY.len() as u64 * changes_total([1, 2])));
	assert_eq!(first.entries[0].value_bytes, ByteSize::from_bytes(POST_BYTES * changes_total([1, 2])));
	assert_eq!(after.min_version().unwrap(), Some(CommitVersion(3)));

	let second = after.drop_before(Cutoff::Version(CommitVersion(5)), 2).unwrap();
	assert_eq!(second.count, Count::new(changes_total([3, 4])));
	assert!(!second.more_remaining, "nothing is left below the cutoff");

	assert_eq!(after.truncated_before().unwrap(), CommitVersion(5));
	assert_eq!(after.min_version().unwrap(), Some(CommitVersion(5)));
	assert_eq!(all_versions(&after), vec![5, 6]);
	for v in 5..=6 {
		assert_record(&after, v);
	}

	let none = after.drop_before(Cutoff::Version(CommitVersion(5)), usize::MAX).unwrap();
	assert_eq!(none.count, Count::ZERO, "a repeated cutoff must not evict a second time");
	assert_eq!(after.truncated_before().unwrap(), CommitVersion(5), "the floor must never move backwards");
}

fn assert_shutdown_flushes_partial_block(tier: &Tier) {
	// shutdown must drain the commit tier first, otherwise every record since the last cut is lost on a clean stop
	let before = tier.store();
	write_all(&before, 1..=3);
	assert!(before.flush_pending());
	write_all(&before, 4..=5);
	before.shutdown();

	let after = tier.store();
	assert_eq!(all_versions(&after), (1..=5).collect::<Vec<_>>());
	assert_eq!(after.max_version().unwrap(), Some(CommitVersion(5)));
	for v in 1..=5 {
		assert_record(&after, v);
	}
	assert!(
		matches!(after.write(&cdc_at(5)), Err(CdcError::DuplicateVersion(CommitVersion(5)))),
		"the flushed partial block seals its versions like any other block"
	);
}

fn assert_drop_without_shutdown_loses_partial_block(tier: &Tier) {
	// a drop never reaches the flusher, so sealed blocks must survive and the unflushed tail must not
	let before = tier.store();
	write_all(&before, 1..=3);
	assert!(before.flush_pending());
	write_all(&before, 4..=5);
	drop(before);

	let after = tier.store();
	assert_eq!(all_versions(&after), vec![1, 2, 3], "only the sealed prefix survives a drop");
	assert_eq!(after.max_version().unwrap(), Some(CommitVersion(3)));
	for v in 1..=3 {
		assert_record(&after, v);
	}
	for lost in 4..=5 {
		assert!(after.read(CommitVersion(lost)).unwrap().is_none(), "v{lost} never reached a block");
	}
	assert!(after.write(&cdc_at(4)).is_ok(), "a version the crash lost is free to be written again");
	assert_record(&after, 4);
}

fn assert_empty_tier_reopens_clean(tier: &Tier) {
	// seeding the seal floor off a missing max version must leave an empty log, never a floor, a bound or a panic
	let before = tier.store();
	assert!(before.min_version().unwrap().is_none());
	assert!(before.max_version().unwrap().is_none());
	before.shutdown();

	let after = tier.store();
	assert!(after.min_version().unwrap().is_none());
	assert!(after.max_version().unwrap().is_none());
	assert_eq!(after.truncated_before().unwrap(), CommitVersion(0));
	assert!(after.read(CommitVersion(1)).unwrap().is_none());
	assert!(all_versions(&after).is_empty());

	assert!(after.write(&cdc_at(1)).is_ok(), "an empty reopen must accept the very first version");
	assert_record(&after, 1);
}

fn assert_log_contiguous_across_restart(tier: &Tier) {
	// the log is one sequence across store lifetimes, so a range spanning the restart has no gap and no repeat
	let before = tier.store();
	write_all(&before, 1..=5);
	assert!(before.flush_pending());
	before.shutdown();

	let after = tier.store();
	write_all(&after, 6..=8);
	assert!(after.flush_pending());
	write_all(&after, 9..=10);

	assert_eq!(all_versions(&after), (1..=10).collect::<Vec<_>>());
	assert_eq!(
		versions_in(&after, Bound::Included(CommitVersion(4)), Bound::Included(CommitVersion(7))),
		vec![4, 5, 6, 7]
	);
	assert_eq!(
		versions_in(&after, Bound::Excluded(CommitVersion(5)), Bound::Included(CommitVersion(9))),
		vec![6, 7, 8, 9]
	);
	assert_eq!(after.min_version().unwrap(), Some(CommitVersion(1)));
	assert_eq!(after.max_version().unwrap(), Some(CommitVersion(10)));
	for v in 1..=10 {
		assert_record(&after, v);
	}

	let batch = after.read_range(Bound::Unbounded, Bound::Unbounded, 6).unwrap();
	assert_eq!(batch.items.len(), 6);
	assert!(batch.has_more, "a bounded batch that stops mid-log must say the rest is still there");
}

macro_rules! restart_tests {
	($mod_name:ident, $case:ident) => {
		mod $mod_name {
			use super::*;

			#[test]
			fn sealed_blocks_survive_restart() {
				$case(super::assert_sealed_blocks_survive_restart);
			}

			#[test]
			fn sealed_version_rejected_after_restart() {
				$case(super::assert_sealed_version_rejected_after_restart);
			}

			#[test]
			fn truncation_floor_survives_restart() {
				$case(super::assert_truncation_floor_survives_restart);
			}

			#[test]
			fn retention_after_restart() {
				$case(super::assert_retention_after_restart);
			}

			#[test]
			fn shutdown_flushes_partial_block() {
				$case(super::assert_shutdown_flushes_partial_block);
			}

			#[test]
			fn drop_without_shutdown_loses_partial_block() {
				$case(super::assert_drop_without_shutdown_loses_partial_block);
			}

			#[test]
			fn empty_tier_reopens_clean() {
				$case(super::assert_empty_tier_reopens_clean);
			}

			#[test]
			fn log_contiguous_across_restart() {
				$case(super::assert_log_contiguous_across_restart);
			}
		}
	};
}

restart_tests!(memory_tier, memory_case);
restart_tests!(sqlite_tier, sqlite_case);
