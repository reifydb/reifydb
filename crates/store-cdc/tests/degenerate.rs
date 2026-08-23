// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::Bound,
	thread,
	time::{Duration as StdDuration, Instant},
};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch, CdcChange},
};
use reifydb_store_cdc::{
	storage::{CdcStorage, Cutoff},
	store::CdcStore,
};
use reifydb_value::{byte_size::ByteSize, count::Count, util::cowvec::CowVec, value::datetime::DateTime};

mod common;

use common::Fixture;

const DEFAULT_TIMESTAMP: u64 = 1_700_000_000_000_000_000;

const HALF_MAX: u64 = u64::MAX / 2;

const MANY_CHANGES: usize = 50_000;

fn cdc_at(version: u64, timestamp: u64, changes: usize) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(timestamp),
		(0..changes)
			.map(|i| CdcChange::Insert {
				key: EncodedKey::new(format!("k{i}").into_bytes()),
				post: EncodedBytes(CowVec::new(format!("v{i}").into_bytes())),
			})
			.collect(),
	)
}

fn cdc_minimal(version: u64) -> Cdc {
	cdc_at(version, DEFAULT_TIMESTAMP, 1)
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

fn version_list(batch: &CdcBatch) -> Vec<u64> {
	batch.items.iter().map(|cdc| cdc.version.0).collect()
}

fn within<T: Send + 'static>(label: &str, seconds: u64, body: impl FnOnce() -> T + Send + 'static) -> T {
	// a saturating cursor turns a bounded walk into an unbounded one, which without a deadline hangs the whole
	// suite
	let handle = thread::spawn(body);
	let deadline = Instant::now() + StdDuration::from_secs(seconds);
	while !handle.is_finished() {
		if Instant::now() >= deadline {
			panic!("{label} did not terminate within {seconds}s");
		}
		thread::sleep(StdDuration::from_millis(20));
	}
	match handle.join() {
		Ok(value) => value,
		Err(payload) => std::panic::resume_unwind(payload),
	}
}

mod cases {
	use super::*;

	pub fn version_zero_round_trip(fixture: Fixture) {
		// version 0 is a real commit version, not a sentinel; a tier that reads 0 as absent loses the first
		// record the log ever carries
		let store = &fixture.store;
		store.write(&cdc_minimal(0)).unwrap();
		let check = |store: &CdcStore| {
			let read = store.read(CommitVersion(0)).unwrap().expect("version 0 must be readable");
			assert_eq!(read.version, CommitVersion(0));
			assert_eq!(store.count(CommitVersion(0)).unwrap(), 1);
			assert_eq!(store.min_version().unwrap(), Some(CommitVersion(0)));
			assert_eq!(store.max_version().unwrap(), Some(CommitVersion(0)));
			let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 16).unwrap();
			assert_eq!(version_list(&batch), vec![0]);
			let batch = store
				.read_range(Bound::Included(CommitVersion(0)), Bound::Included(CommitVersion(0)), 16)
				.unwrap();
			assert_eq!(version_list(&batch), vec![0]);
			let batch = store.read_range(Bound::Excluded(CommitVersion(0)), Bound::Unbounded, 16).unwrap();
			assert!(version_list(&batch).is_empty(), "an exclusive start at 0 must skip version 0");
		};
		check(store);
		assert!(store.flush_pending());
		check(store);
	}

	pub fn version_zero_drop_before(fixture: Fixture) {
		// a cutoff of 0 has nothing below it, so it must be a no-op rather than advance the floor past the
		// record at 0
		let store = &fixture.store;
		seal_each(store, [0u64, 1, 2]);

		let r = store.drop_before(Cutoff::Version(CommitVersion(0)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::ZERO);
		assert!(r.entries.is_empty());
		assert!(!r.more_remaining);
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(0));
		assert!(store.read(CommitVersion(0)).unwrap().is_some());

		let r = store.drop_before(Cutoff::Version(CommitVersion(1)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::new(1));
		assert!(store.read(CommitVersion(0)).unwrap().is_none());
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(1));
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(1)));
	}

	pub fn version_zero_excluded_end(fixture: Fixture) {
		// no version sits below 0, so an exclusive end at 0 selects nothing; saturating normalization must not
		// turn the excluded bound into an included one
		let store = &fixture.store;
		seal_each(store, [0u64, 1]);
		let batch = store.read_range(Bound::Unbounded, Bound::Excluded(CommitVersion(0)), 16).unwrap();
		let observed = version_list(&batch);
		assert!(observed.is_empty(), "an exclusive end at 0 must exclude version 0, got {observed:?}");
		assert!(!batch.has_more);
	}

	pub fn version_max_round_trip(fixture: Fixture) {
		// the top of the version space must be storable and readable like any other version, before and after
		// the flush
		let store = &fixture.store;
		store.write(&cdc_minimal(u64::MAX)).unwrap();
		let check = |store: &CdcStore| {
			let read = store.read(CommitVersion(u64::MAX)).unwrap().expect("u64::MAX must be readable");
			assert_eq!(read.version, CommitVersion(u64::MAX));
			assert_eq!(store.count(CommitVersion(u64::MAX)).unwrap(), 1);
			assert_eq!(store.min_version().unwrap(), Some(CommitVersion(u64::MAX)));
			assert_eq!(store.max_version().unwrap(), Some(CommitVersion(u64::MAX)));
			let batch = store
				.read_range(
					Bound::Included(CommitVersion(u64::MAX)),
					Bound::Included(CommitVersion(u64::MAX)),
					1,
				)
				.unwrap();
			assert_eq!(version_list(&batch), vec![u64::MAX]);
			assert!(!batch.has_more);
		};
		check(store);
		assert!(store.flush_pending());
		check(store);
	}

	pub fn version_max_cursor_advances(fixture: Fixture) {
		// the walk advances with max_version + 1, which saturates on a block ending at u64::MAX, so that block
		// must be re-emitted until the batch fills
		let store = fixture.store.clone();
		seal_each(&store, [1u64, 2, 3, u64::MAX]);
		let observed = within("read_range past a block ending at u64::MAX", 30, move || {
			version_list(&store.read_range(Bound::Unbounded, Bound::Unbounded, 32).unwrap())
		});
		assert_eq!(observed, vec![1, 2, 3, u64::MAX], "the walk must emit every version exactly once");
	}

	pub fn version_max_excluded_start(fixture: Fixture) {
		// no version sits above u64::MAX, so an exclusive start there selects nothing; saturating normalization
		// must not turn the excluded bound into an included one
		let store = fixture.store.clone();
		seal_each(&store, [1u64, u64::MAX]);
		let observed = within("read_range from an exclusive u64::MAX start", 30, move || {
			version_list(
				&store.read_range(Bound::Excluded(CommitVersion(u64::MAX)), Bound::Unbounded, 8)
					.unwrap(),
			)
		});
		assert!(
			observed.is_empty(),
			"an exclusive start at u64::MAX must exclude every version, got {observed:?}"
		);
	}

	pub fn version_max_excluded_end(fixture: Fixture) {
		// an exclusive end at u64::MAX is the one saturating case with a representable answer, and the walk
		// must stop before the block it names
		let store = &fixture.store;
		seal_each(store, [1u64, 2, u64::MAX]);
		let batch = store.read_range(Bound::Unbounded, Bound::Excluded(CommitVersion(u64::MAX)), 16).unwrap();
		assert_eq!(version_list(&batch), vec![1, 2]);
		assert!(!batch.has_more);
	}

	pub fn ttl_cutoff_at_max_version(fixture: Fixture) {
		// with nothing at or after the cutoff the answer saturates at u64::MAX, a version that still exists, so
		// a sweep driven by it can never expire the last block
		let store = &fixture.store;
		store.write(&cdc_at(u64::MAX, 100, 1)).unwrap();
		assert!(store.flush_pending());

		let cutoff = store
			.find_ttl_cutoff(DateTime::from_nanos(500))
			.unwrap()
			.expect("a populated tier always answers a cutoff");
		let dropped = store.drop_before(cutoff, usize::MAX).unwrap();
		assert_eq!(dropped.count, Count::new(1), "a cutoff above every timestamp must expire every record");
		assert!(store.read(CommitVersion(u64::MAX)).unwrap().is_none());
		assert!(store.min_version().unwrap().is_none());
	}

	pub fn batch_size_zero_populated(fixture: Fixture) {
		// a zero batch asks for nothing, so it must hand back nothing while still reporting the range as not
		// exhausted
		let store = &fixture.store;
		seal_each(store, 1..=3u64);
		write_all(store, 4..=5u64);
		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 0).unwrap();
		assert!(batch.items.is_empty());
		assert!(batch.has_more, "records remain unread, so the range is not exhausted");
	}

	pub fn batch_size_zero_empty_store(fixture: Fixture) {
		// has_more is the only signal a polling consumer has; claiming more on an empty tier turns a zero batch
		// into a spin that never terminates
		let store = &fixture.store;
		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 0).unwrap();
		assert!(batch.items.is_empty());
		assert!(!batch.has_more, "an empty tier holds nothing more to read");
	}

	pub fn batch_size_max_terminates(fixture: Fixture) {
		// batch_size u64::MAX becomes a want of usize::MAX, and the walk must stop when the tiers run out
		// rather than keep asking for a cursor that no longer advances
		let store = fixture.store.clone();
		seal_each(&store, 1..=5u64);
		write_all(&store, 6..=10u64);
		let (versions, has_more) = within("read_range with a u64::MAX batch size", 30, move || {
			let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, u64::MAX).unwrap();
			(version_list(&batch), batch.has_more)
		});
		assert_eq!(versions, (1..=10).collect::<Vec<_>>());
		assert!(!has_more);
	}

	pub fn drop_before_limit_zero_empty(fixture: Fixture) {
		// a zero budget on an empty tier must report nothing left, otherwise a retention loop keyed on
		// more_remaining never finishes
		let store = &fixture.store;
		let r = store.drop_before(Cutoff::Version(CommitVersion(10)), 0).unwrap();
		assert_eq!(r.count, Count::ZERO);
		assert!(r.entries.is_empty());
		assert!(!r.more_remaining);
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(0));
	}

	pub fn drop_before_limit_zero_populated(fixture: Fixture) {
		// a zero budget must delete nothing and must not advance the floor, or a caller that throttles
		// retention to zero loses records it refused to pay for
		let store = &fixture.store;
		seal_each(store, 1..=5u64);
		let r = store.drop_before(Cutoff::Version(CommitVersion(4)), 0).unwrap();
		assert_eq!(r.count, Count::ZERO);
		assert!(r.entries.is_empty());
		assert!(r.more_remaining, "blocks below the cutoff survive, so the pass is unfinished");
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(0));
		for v in 1..=5u64 {
			assert!(store.read(CommitVersion(v)).unwrap().is_some(), "v{v} must survive a zero budget");
		}
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(1)));
	}

	pub fn drop_before_limit_max_empty(fixture: Fixture) {
		// the scan takes limit + 1 to learn whether more remain, and usize::MAX is where that addition
		// overflows
		let store = &fixture.store;
		let r = store.drop_before(Cutoff::Version(CommitVersion(10)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::ZERO);
		assert!(r.entries.is_empty());
		assert!(!r.more_remaining);
	}

	pub fn drop_before_limit_max_populated(fixture: Fixture) {
		// the same limit + 1 overflow, but with blocks present so the scan actually walks and reports
		let store = &fixture.store;
		seal_each(store, 1..=5u64);
		let r = store.drop_before(Cutoff::Version(CommitVersion(4)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::new(3));
		assert!(!r.more_remaining);
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(4));
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(4)));
	}

	pub fn zero_change_record(fixture: Fixture) {
		// an empty block is rejected because it has no version range, but a record carrying no changes must
		// still seal, read back, and count as zero
		let store = &fixture.store;
		store.write(&cdc_at(7, 100, 0)).unwrap();
		let read = store.read(CommitVersion(7)).unwrap().expect("a zero-change record must be readable");
		assert!(read.changes.is_empty());
		assert_eq!(store.count(CommitVersion(7)).unwrap(), 0);

		assert!(store.flush_pending());
		let summaries = fixture.persistent.summaries_from(CommitVersion(0), 16).unwrap();
		assert_eq!(summaries.len(), 1, "a zero-change record must still seal into a block");
		assert_eq!(summaries[0].count, Count::new(1));
		assert_eq!(summaries[0].min_version, CommitVersion(7));
		assert_eq!(summaries[0].max_version, CommitVersion(7));
		assert!(store.read(CommitVersion(7)).unwrap().is_some());
		assert_eq!(store.count(CommitVersion(7)).unwrap(), 0);
		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 16).unwrap();
		assert_eq!(version_list(&batch), vec![7]);

		let r = store.drop_before(Cutoff::Version(CommitVersion(8)), usize::MAX).unwrap();
		assert!(r.entries.is_empty(), "no change means no per-source rollup to subtract");
		assert_eq!(r.count, Count::ZERO);
		assert!(!r.more_remaining);
		assert!(
			store.read(CommitVersion(7)).unwrap().is_none(),
			"the block must drop even when it rolls up to zero"
		);
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(8));
	}

	pub fn many_changes_record(fixture: Fixture) {
		// the rollup is charged per change, so a record far larger than any block boundary must still round
		// trip intact and be counted exactly once on eviction
		let store = &fixture.store;
		store.write(&cdc_at(1, 100, MANY_CHANGES)).unwrap();
		assert_eq!(store.count(CommitVersion(1)).unwrap(), MANY_CHANGES);
		assert!(store.flush_pending());
		assert_eq!(
			store.count(CommitVersion(1)).unwrap(),
			MANY_CHANGES,
			"the payload must survive the round trip"
		);

		let r = store.drop_before(Cutoff::Version(CommitVersion(2)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::new(MANY_CHANGES as u64));
		let rolled: u64 = r.entries.iter().map(|entry| entry.count.as_u64()).sum();
		assert_eq!(rolled, MANY_CHANGES as u64, "every change must land in exactly one per-source entry");
	}

	pub fn empty_key_and_value(fixture: Fixture) {
		// a zero-length key still resolves to a metrics source and a zero-length value still is a change, so
		// the rollup must count it while charging zero bytes
		let store = &fixture.store;
		let cdc = Cdc::new(
			CommitVersion(1),
			DateTime::from_nanos(100),
			vec![CdcChange::Insert {
				key: EncodedKey::new(vec![]),
				post: EncodedBytes(CowVec::new(vec![])),
			}],
		);
		store.write(&cdc).unwrap();
		assert_eq!(store.count(CommitVersion(1)).unwrap(), 1);
		assert!(store.flush_pending());

		let read = store.read(CommitVersion(1)).unwrap().expect("an empty key must survive the round trip");
		assert_eq!(read.changes.len(), 1);
		assert_eq!(read.changes[0].key().len(), 0);
		assert_eq!(read.changes[0].value_bytes(), 0);

		let r = store.drop_before(Cutoff::Version(CommitVersion(2)), usize::MAX).unwrap();
		assert_eq!(r.count, Count::new(1));
		assert_eq!(r.entries.len(), 1);
		assert_eq!(r.entries[0].key_bytes, ByteSize::ZERO);
		assert_eq!(r.entries[0].value_bytes, ByteSize::ZERO);
		assert_eq!(r.entries[0].count, Count::new(1));
	}

	pub fn drop_before_idempotent_floor(fixture: Fixture) {
		// the truncation floor is monotonic: a later pass below an already-advanced floor must find nothing and
		// must never hand back a range retention already removed
		let store = &fixture.store;
		seal_each(store, 1..=6u64);
		let first = store.drop_before(Cutoff::Version(CommitVersion(4)), usize::MAX).unwrap();
		assert_eq!(first.count, Count::new(3));
		assert_eq!(store.truncated_before().unwrap(), CommitVersion(4));

		for cutoff in [0u64, 1, 2, 3, 4] {
			let again = store.drop_before(Cutoff::Version(CommitVersion(cutoff)), usize::MAX).unwrap();
			assert_eq!(again.count, Count::ZERO, "cutoff {cutoff} sits at or below the floor");
			assert!(again.entries.is_empty(), "cutoff {cutoff} must roll up nothing");
			assert!(!again.more_remaining, "cutoff {cutoff} has no block left below it");
			assert_eq!(
				store.truncated_before().unwrap(),
				CommitVersion(4),
				"cutoff {cutoff} must not move the floor backwards"
			);
		}
		assert_eq!(store.min_version().unwrap(), Some(CommitVersion(4)));
		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 16).unwrap();
		assert_eq!(version_list(&batch), vec![4, 5, 6]);
	}

	pub fn non_monotonic_timestamps_in_block(fixture: Fixture) {
		// versions ascend while timestamps descend inside one block, so a summary must track true min/max
		// timestamps, not merely the first and last entry
		let store = &fixture.store;
		store.write(&cdc_at(1, 300, 1)).unwrap();
		store.write(&cdc_at(2, 200, 1)).unwrap();
		store.write(&cdc_at(3, 100, 1)).unwrap();
		assert!(store.flush_pending());

		let summaries = fixture.persistent.summaries_from(CommitVersion(0), 16).unwrap();
		assert_eq!(summaries.len(), 1);
		assert_eq!(summaries[0].min_version, CommitVersion(1));
		assert_eq!(summaries[0].max_version, CommitVersion(3));
		assert_eq!(summaries[0].min_timestamp.to_nanos(), 100, "the summary must carry the lowest timestamp");
		assert_eq!(summaries[0].max_timestamp.to_nanos(), 300, "the summary must carry the highest timestamp");

		assert_eq!(
			store.find_ttl_cutoff(DateTime::from_nanos(50)).unwrap(),
			Some(Cutoff::Version(CommitVersion(1)))
		);
		assert_eq!(
			store.find_ttl_cutoff(DateTime::from_nanos(250)).unwrap(),
			Some(Cutoff::Version(CommitVersion(1))),
			"the block still holds a record at or after the cutoff, so the whole block is retained"
		);
		assert_eq!(
			store.find_ttl_cutoff(DateTime::from_nanos(301)).unwrap(),
			Some(Cutoff::Unbounded),
			"nothing survives the cutoff, so every block is expired and no version can name the bound"
		);
	}

	pub fn huge_version_gap_walk(fixture: Fixture) {
		// the walk must step block to block through summaries; a cursor advancing one version at a time would
		// need 2^63 iterations to cross this gap
		let store = fixture.store.clone();
		seal_each(&store, [1u64, HALF_MAX]);

		let spanning = within("read_range across a 2^63 version gap", 30, {
			let store = store.clone();
			move || version_list(&store.read_range(Bound::Unbounded, Bound::Unbounded, 100).unwrap())
		});
		assert_eq!(spanning, vec![1, HALF_MAX]);

		let inside = within("read_range inside a 2^63 version gap", 30, move || {
			version_list(
				&store.read_range(
					Bound::Included(CommitVersion(2)),
					Bound::Included(CommitVersion(HALF_MAX - 1)),
					100,
				)
				.unwrap(),
			)
		});
		assert!(inside.is_empty(), "the gap holds no versions, got {inside:?}");
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
		version_zero_round_trip,
		version_zero_drop_before,
		version_zero_excluded_end,
		version_max_round_trip,
		version_max_cursor_advances,
		version_max_excluded_start,
		version_max_excluded_end,
		ttl_cutoff_at_max_version,
		batch_size_zero_populated,
		batch_size_zero_empty_store,
		batch_size_max_terminates,
		drop_before_limit_zero_empty,
		drop_before_limit_zero_populated,
		drop_before_limit_max_empty,
		drop_before_limit_max_populated,
		zero_change_record,
		many_changes_record,
		empty_key_and_value,
		drop_before_idempotent_floor,
		non_monotonic_timestamps_in_block,
		huge_version_gap_walk,
	]
);
