// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_store_cdc::{
	error::CdcError,
	storage::{CdcStorage, Cutoff, DropBeforeResult},
	store::CdcStore,
};
use reifydb_value::{byte_size::ByteSize, count::Count, util::cowvec::CowVec, value::datetime::DateTime};

mod common;

use common::Fixture;

fn cdc_minimal(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		vec![CdcChange::Insert {
			key: EncodedKey::new(vec![1, 2, 3]),
			post: EncodedBytes(CowVec::new(vec![10, 20, 30])),
		}],
	)
}

fn write_all(store: &CdcStore, versions: impl IntoIterator<Item = u64>) {
	for v in versions {
		store.write(&cdc_minimal(v)).unwrap();
	}
}

fn seal_each(store: &CdcStore, versions: impl IntoIterator<Item = u64>) {
	// one flush per record puts each version in a block of its own, so a cutoff can land between two blocks
	for v in versions {
		store.write(&cdc_minimal(v)).unwrap();
		assert!(store.flush_pending());
	}
}

mod cases {
	use super::*;

	pub fn write_read_round_trip(fixture: Fixture) {
		let store = fixture.store;
		let check = |store: &CdcStore| {
			let read = store.read(CommitVersion(1)).unwrap().expect("entry should exist");
			assert_eq!(read.version, CommitVersion(1));
			assert_eq!(read.changes.len(), 1);
		};
		store.write(&cdc_minimal(1)).unwrap();
		check(&store);
		assert!(store.flush_pending());
		check(&store);
	}

	pub fn read_nonexistent(fixture: Fixture) {
		let store = fixture.store;
		assert!(store.read(CommitVersion(999)).unwrap().is_none());
		write_all(&store, 1..=3);
		assert!(store.read(CommitVersion(999)).unwrap().is_none());
		assert!(store.flush_pending());
		assert!(store.read(CommitVersion(999)).unwrap().is_none());
	}

	pub fn range_inclusive(fixture: Fixture) {
		let store = fixture.store;
		let check = |store: &CdcStore| {
			let batch = store
				.read_range(Bound::Included(CommitVersion(3)), Bound::Included(CommitVersion(7)), 100)
				.unwrap();
			assert_eq!(batch.items.len(), 5);
			assert!(!batch.has_more);
			assert_eq!(batch.items[0].version, CommitVersion(3));
			assert_eq!(batch.items[4].version, CommitVersion(7));
		};
		write_all(&store, 1..=10);
		check(&store);
		assert!(store.flush_pending());
		check(&store);
	}

	pub fn range_exclusive(fixture: Fixture) {
		let store = fixture.store;
		let check = |store: &CdcStore| {
			let batch = store
				.read_range(Bound::Excluded(CommitVersion(2)), Bound::Included(CommitVersion(4)), 100)
				.unwrap();
			assert_eq!(batch.items.len(), 2);
			assert_eq!(batch.items[0].version, CommitVersion(3));
			assert_eq!(batch.items[1].version, CommitVersion(4));

			let batch = store
				.read_range(Bound::Included(CommitVersion(2)), Bound::Excluded(CommitVersion(4)), 100)
				.unwrap();
			assert_eq!(batch.items.len(), 2);
			assert_eq!(batch.items[0].version, CommitVersion(2));
			assert_eq!(batch.items[1].version, CommitVersion(3));
		};
		write_all(&store, 1..=5);
		check(&store);
		assert!(store.flush_pending());
		check(&store);
	}

	pub fn range_batch_size_has_more(fixture: Fixture) {
		let store = fixture.store;
		let check = |store: &CdcStore| {
			let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 3).unwrap();
			assert_eq!(batch.items.len(), 3);
			assert!(batch.has_more);
		};
		write_all(&store, 1..=10);
		check(&store);
		assert!(store.flush_pending());
		check(&store);
	}

	pub fn range_spans_commit_and_blocks(fixture: Fixture) {
		let store = fixture.store;
		// a range straddling the handoff from blocks to the commit buffer must return one contiguous run with
		// no gap and no repeat
		write_all(&store, 1..=5);
		assert!(store.flush_pending());
		write_all(&store, 6..=10);

		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 100).unwrap();
		let versions: Vec<u64> = batch.items.iter().map(|cdc| cdc.version.0).collect();
		assert_eq!(versions, (1..=10).collect::<Vec<_>>());
		assert!(!batch.has_more);

		let batch = store
			.read_range(Bound::Included(CommitVersion(4)), Bound::Included(CommitVersion(7)), 100)
			.unwrap();
		let versions: Vec<u64> = batch.items.iter().map(|cdc| cdc.version.0).collect();
		assert_eq!(versions, vec![4, 5, 6, 7]);
	}

	pub fn count(fixture: Fixture) {
		let store = fixture.store;
		let cdc = Cdc::new(
			CommitVersion(1),
			DateTime::from_nanos(1),
			(0..5).map(|i| CdcChange::Insert {
				key: EncodedKey::new(vec![i as u8]),
				post: EncodedBytes(CowVec::new(vec![])),
			})
			.collect(),
		);
		store.write(&cdc).unwrap();
		assert_eq!(store.count(CommitVersion(1)).unwrap(), 5);
		assert_eq!(store.count(CommitVersion(2)).unwrap(), 0);
		assert!(store.flush_pending());
		assert_eq!(store.count(CommitVersion(1)).unwrap(), 5);
		assert_eq!(store.count(CommitVersion(2)).unwrap(), 0);
	}

	pub fn min_max_version(fixture: Fixture) {
		let store = fixture.store;
		assert!(store.min_version().unwrap().is_none());
		assert!(store.max_version().unwrap().is_none());

		write_all(&store, [5u64, 3, 8]);

		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(3)));
		assert_eq!(store.max_version().unwrap(), Some(CommitVersion(8)));
		assert!(store.flush_pending());
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(3)));
		assert_eq!(store.max_version().unwrap(), Some(CommitVersion(8)));
	}

	pub fn min_max_version_across_tiers(fixture: Fixture) {
		let store = fixture.store;
		// the oldest version only ever lives in a sealed block, the newest only in the commit buffer, so
		// answering either from the wrong tier silently narrows the range
		write_all(&store, 1..=3);
		assert!(store.flush_pending());
		write_all(&store, 4..=6);

		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(1)));
		assert_eq!(store.max_version().unwrap(), Some(CommitVersion(6)));

		// retention moves the floor up while the commit tier is still ahead, and both ends must follow
		store.drop_before(Cutoff::Version(CommitVersion(4)), usize::MAX).unwrap();
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(4)));
		assert_eq!(store.max_version().unwrap(), Some(CommitVersion(6)));
	}

	pub fn duplicate_version_rejected(fixture: Fixture) {
		let store = fixture.store;
		// the log carries exactly one record per commit version, so a second write of the same version must
		// surface rather than silently duplicate or drop
		store.write(&cdc_minimal(1)).unwrap();
		assert!(matches!(store.write(&cdc_minimal(1)), Err(CdcError::DuplicateVersion(CommitVersion(1)))));

		assert!(store.flush_pending());
		assert!(matches!(store.write(&cdc_minimal(1)), Err(CdcError::DuplicateVersion(CommitVersion(1)))));

		store.write(&cdc_minimal(2)).unwrap();
		assert_eq!(store.read(CommitVersion(2)).unwrap().unwrap().version, CommitVersion(2));
	}

	pub fn drop_before_empty(fixture: Fixture) {
		let store = fixture.store;
		let r = store.drop_before(Cutoff::Version(CommitVersion(10)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::ZERO);
		assert!(r.entries.is_empty());
	}

	pub fn drop_before_some(fixture: Fixture) {
		let store = fixture.store;
		seal_each(&store, [1u64, 3, 5, 7, 9]);
		let r = store.drop_before(Cutoff::Version(CommitVersion(5)), usize::MAX).unwrap();
		// versions 1 and 3 are evicted and carry the same key, so they roll up into a single per-source entry
		// of count 2, not two entries of 1
		assert_eq!(r.count, Count::new(2));
		assert_eq!(r.entries.len(), 1);
		assert_eq!(r.entries[0].count, Count::new(2));
		assert!(store.read(CommitVersion(1)).unwrap().is_none());
		assert!(store.read(CommitVersion(3)).unwrap().is_none());
		assert!(store.read(CommitVersion(5)).unwrap().is_some());
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(5)));
	}

	pub fn drop_before_all(fixture: Fixture) {
		let store = fixture.store;
		seal_each(&store, 1..=3u64);
		let r = store.drop_before(Cutoff::Version(CommitVersion(10)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::new(3));
		assert!(store.min_version().unwrap().is_none());
	}

	pub fn drop_before_none_when_too_low(fixture: Fixture) {
		let store = fixture.store;
		seal_each(&store, 5..=7u64);
		let r = store.drop_before(Cutoff::Version(CommitVersion(3)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::ZERO);
		assert!(r.entries.is_empty());
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(5)));
	}

	pub fn drop_before_boundary(fixture: Fixture) {
		let store = fixture.store;
		seal_each(&store, 1..=5u64);
		let r = store.drop_before(Cutoff::Version(CommitVersion(3)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::new(2));
		assert!(store.read(CommitVersion(3)).unwrap().is_some());
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(3)));
	}

	pub fn drop_before_retains_straddling_block(fixture: Fixture) {
		let store = fixture.store;
		// retention only ever deletes whole blocks, so a cutoff inside a block must leave that block intact
		// rather than rewrite it
		write_all(&store, 1..=5);
		assert!(store.flush_pending());

		let r = store.drop_before(Cutoff::Version(CommitVersion(3)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::ZERO);
		assert!(!r.more_remaining);
		for v in 1..=5 {
			assert!(store.read(CommitVersion(v)).unwrap().is_some());
		}
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(0));
	}

	pub fn drop_before_entry_stats(fixture: Fixture) {
		let store = fixture.store;
		let cdc = Cdc::new(
			CommitVersion(1),
			DateTime::from_nanos(12345),
			vec![CdcChange::Insert {
				key: EncodedKey::new(vec![1, 2, 3]),
				post: EncodedBytes(CowVec::new(vec![10, 20, 30, 40, 50])),
			}],
		);
		store.write(&cdc).unwrap();
		assert!(store.flush_pending());
		let r: DropBeforeResult = store.drop_before(Cutoff::Version(CommitVersion(2)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::new(1));
		assert_eq!(r.entries.len(), 1);
		// the rollup is charged from the change itself, not the stored payload, so a compressed tier and an
		// uncompressed one must report the same bytes
		assert_eq!(r.entries[0].key_bytes, ByteSize::from_bytes(3));
		assert_eq!(r.entries[0].value_bytes, ByteSize::from_bytes(5));
		assert_eq!(r.entries[0].count, Count::new(1));
	}

	pub fn drop_before_limited(fixture: Fixture) {
		let store = fixture.store;
		seal_each(&store, 1..=12u64);
		let cutoff = CommitVersion(11);

		// a bounded pass deletes at most `limit` below-cutoff blocks and reports that more remain
		let first = store.drop_before(Cutoff::Version(cutoff), 4).unwrap();
		assert_eq!(first.count, Count::new(4));
		assert!(first.more_remaining);
		assert_eq!(first.entries.len(), 1);
		assert_eq!(first.entries[0].count, Count::new(4));
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(5)));

		assert!(store.read(CommitVersion(11)).unwrap().is_some());
		assert!(store.read(CommitVersion(12)).unwrap().is_some());

		let mut total = first.count;
		loop {
			let r = store.drop_before(Cutoff::Version(cutoff), 4).unwrap();
			total = total.saturating_add(r.count);
			if !r.more_remaining {
				break;
			}
		}
		assert_eq!(total, Count::new(10));
		assert!(store.read(CommitVersion(10)).unwrap().is_none());
		assert!(store.read(CommitVersion(11)).unwrap().is_some());
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(11)));
	}

	pub fn read_range_skips_truncated_prefix(fixture: Fixture) {
		let store = fixture.store;
		// a read starting below the retained floor must step over the hole, otherwise a lagging consumer stalls
		// forever on a version retention already removed
		seal_each(&store, 1..=6u64);
		store.drop_before(Cutoff::Version(CommitVersion(4)), usize::MAX).unwrap();

		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 100).unwrap();
		let versions: Vec<u64> = batch.items.iter().map(|cdc| cdc.version.0).collect();
		assert_eq!(versions, vec![4, 5, 6]);
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(4));
	}

	pub fn range_inverted_returns_empty(fixture: Fixture) {
		let store = fixture.store;
		write_all(&store, 1..=5);
		assert!(store.flush_pending());
		let batch = store
			.read_range(Bound::Included(CommitVersion(10)), Bound::Included(CommitVersion(5)), 16)
			.expect("inverted range must not error");
		assert!(batch.items.is_empty(), "inverted range must return no items");
		assert!(!batch.has_more, "inverted range cannot have more items");
	}

	pub fn range_excluded_zero_end_returns_empty(fixture: Fixture) {
		let store = fixture.store;
		write_all(&store, 1..=3);
		assert!(store.flush_pending());
		let batch = store
			.read_range(Bound::Unbounded, Bound::Excluded(CommitVersion(0)), 16)
			.expect("Excluded(0) end must not panic");
		assert!(batch.items.is_empty());
		assert!(!batch.has_more);
	}

	pub fn range_excluded_pair_collapsing(fixture: Fixture) {
		let store = fixture.store;
		write_all(&store, 1..=10);
		assert!(store.flush_pending());
		let batch = store
			.read_range(Bound::Excluded(CommitVersion(5)), Bound::Excluded(CommitVersion(6)), 16)
			.expect("collapsing exclusive bounds must not panic");
		assert!(batch.items.is_empty());
		assert!(!batch.has_more);
	}
}

crate::tier_tests!(
	[
		memory = common::memory,
		memory_cached = common::memory_cached,
		sqlite = common::sqlite,
		sqlite_cached = common::sqlite_cached,
		sqlite_starved_cache = common::sqlite_starved_cache,
	],
	[
		write_read_round_trip,
		read_nonexistent,
		range_inclusive,
		range_exclusive,
		range_batch_size_has_more,
		range_spans_commit_and_blocks,
		count,
		min_max_version,
		min_max_version_across_tiers,
		duplicate_version_rejected,
		drop_before_empty,
		drop_before_some,
		drop_before_all,
		drop_before_none_when_too_low,
		drop_before_boundary,
		drop_before_retains_straddling_block,
		drop_before_entry_stats,
		drop_before_limited,
		read_range_skips_truncated_prefix,
		range_inverted_returns_empty,
		range_excluded_zero_end_returns_empty,
		range_excluded_pair_collapsing,
	]
);
