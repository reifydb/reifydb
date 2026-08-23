// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::Bound,
	thread::sleep,
	time::{Duration as StdDuration, Instant},
};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_cdc::{
	storage::CdcStorage,
	store::CdcStore,
	tier::{commit::CdcCommitBufferTier, persistent::CdcPersistentTier, read::CdcReadConfig},
	types::cdc_resident_bytes,
};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::datetime::DateTime};

mod common;

use common::Fixture;

const KEY_LEN: usize = 4;

const PAYLOAD_SLACK: usize = 64;

const CEILING: ByteSize = ByteSize::from_mib(256);

const SUMMARY_LIMIT: usize = 1024;

const TIMESTAMP_BASE: u64 = 1_700_000_000_000_000_000;

const AUTO_CUT_TIMEOUT: StdDuration = StdDuration::from_secs(5);

/// Byte cost of the smallest record these tests write; every other record is an exact multiple of it, so a block
/// boundary is arithmetic rather than a guess.
fn unit_bytes() -> usize {
	size_of::<Cdc>() + KEY_LEN + PAYLOAD_SLACK
}

fn unit() -> ByteSize {
	ByteSize::from_bytes(unit_bytes() as u64)
}

fn record(version: u64, units: usize) -> Cdc {
	let payload = units * unit_bytes() - size_of::<Cdc>() - KEY_LEN;
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(TIMESTAMP_BASE + version),
		vec![CdcChange::Insert {
			key: EncodedKey::new(vec![b'k'; KEY_LEN]),
			post: EncodedBytes(CowVec::new(vec![0xab; payload])),
		}],
	)
}

fn assert_unit_model() {
	// every expected layout below is derived from this identity; if the store ever charges a record differently,
	// fail here rather than pass by accident
	assert_eq!(cdc_resident_bytes(&record(1, 1)), unit(), "a one-unit record must cost exactly one unit");
	assert_eq!(cdc_resident_bytes(&record(1, 4)), unit() * 4, "record cost must scale with the payload");
}

#[derive(Clone, Copy)]
enum Tier {
	Memory,
	Sqlite,
}

#[derive(Clone, Copy)]
enum Cache {
	Absent,
	Default,
	Starved,
}

#[derive(Clone, Copy)]
struct Combination {
	tier: Tier,
	cache: Cache,
}

fn memory() -> Combination {
	Combination {
		tier: Tier::Memory,
		cache: Cache::Absent,
	}
}

fn memory_cached() -> Combination {
	Combination {
		tier: Tier::Memory,
		cache: Cache::Default,
	}
}

fn sqlite() -> Combination {
	Combination {
		tier: Tier::Sqlite,
		cache: Cache::Absent,
	}
}

fn sqlite_cached() -> Combination {
	Combination {
		tier: Tier::Sqlite,
		cache: Cache::Default,
	}
}

fn sqlite_starved_cache() -> Combination {
	Combination {
		tier: Tier::Sqlite,
		cache: Cache::Starved,
	}
}

/// Builds one tier combination over a commit buffer sized in whole records, handing back the buffer so a test can hold
/// the flush guard and decide when a cut may happen.
fn fixture(combination: Combination, cut_bytes: ByteSize) -> (Fixture, CdcCommitBufferTier) {
	let (persistent, guard) = match combination.tier {
		Tier::Memory => (CdcPersistentTier::memory(), None),
		Tier::Sqlite => {
			let (config, guard) = SqliteConfig::in_memory();
			(CdcPersistentTier::sqlite(config), Some(guard))
		}
	};
	let read = match combination.cache {
		Cache::Absent => None,
		Cache::Default => Some(CdcReadConfig::default()),
		Cache::Starved => Some(CdcReadConfig {
			resident_bytes: Some(ByteSize::from_bytes(1)),
			shards: 1,
		}),
	};
	let commit = common::commit_config(cut_bytes, CEILING);
	let buffer = commit.storage.clone();
	(common::custom(persistent, read, commit, guard), buffer)
}

fn write_sizes(store: &CdcStore, sizes: &[usize]) {
	for (index, units) in sizes.iter().enumerate() {
		store.write(&record(index as u64 + 1, *units)).unwrap();
	}
}

fn layout(fixture: &Fixture) -> Vec<(u64, u64, u64)> {
	fixture.persistent
		.summaries_from(CommitVersion(0), SUMMARY_LIMIT)
		.unwrap()
		.iter()
		.map(|summary| (summary.min_version.0, summary.max_version.0, summary.count.as_u64()))
		.collect()
}

/// Writes the run with the flusher held back, so the block layout is decided by byte accounting alone, never by when
/// the flush actor happened to wake.
fn write_then_cut(fixture: &Fixture, buffer: &CdcCommitBufferTier, sizes: &[usize]) {
	let held = buffer.flush_guard();
	write_sizes(&fixture.store, sizes);
	assert!(layout(fixture).is_empty(), "no block may be sealed while the flusher is held back");
	drop(held);
	assert!(fixture.store.flush_pending(), "flush must not time out");
}

mod cases {
	use super::*;

	pub fn flush_emits_several_blocks(combination: Combination) {
		// one flush over a buffer more than three times the cut size must emit a block per cut worth of bytes,
		// not one block holding everything
		assert_unit_model();
		let (fixture, buffer) = fixture(combination, unit() * 3);

		write_then_cut(&fixture, &buffer, &[1; 10]);

		assert_eq!(layout(&fixture), vec![(1, 3, 3), (4, 6, 3), (7, 9, 3), (10, 10, 1)]);
	}

	pub fn block_boundaries_follow_byte_accounting(combination: Combination) {
		// a boundary lands where the running byte total says it does, never on a record count
		assert_unit_model();
		let (fixture, buffer) = fixture(combination, unit() * 4);

		write_then_cut(&fixture, &buffer, &[1, 1, 3, 1, 2, 4, 1]);

		assert_eq!(layout(&fixture), vec![(1, 2, 2), (3, 4, 2), (5, 5, 1), (6, 6, 1), (7, 7, 1)]);
	}

	pub fn oversized_record_forms_its_own_block(combination: Combination) {
		// a cut must always take at least one record, otherwise a record larger than the cut size could never
		// leave the buffer and the flush loop would spin forever
		assert_unit_model();
		let (fixture, buffer) = fixture(combination, unit() * 2);

		write_then_cut(&fixture, &buffer, &[1, 5, 1, 1, 1]);

		assert_eq!(layout(&fixture), vec![(1, 1, 1), (2, 2, 1), (3, 4, 2), (5, 5, 1)]);
	}

	pub fn boundary_record_joins_the_block(combination: Combination) {
		// the cut compares the total after the record against the cut size, so a record landing exactly on it
		// still joins the block
		assert_unit_model();

		let (exact, exact_buffer) = fixture(combination, unit() * 3);
		write_then_cut(&exact, &exact_buffer, &[1; 6]);
		assert_eq!(
			layout(&exact),
			vec![(1, 3, 3), (4, 6, 3)],
			"a record landing exactly on the cut belongs to the block"
		);

		let (short, short_buffer) = fixture(combination, ByteSize::from_bytes(unit_bytes() as u64 * 3 - 1));
		write_then_cut(&short, &short_buffer, &[1; 6]);
		assert_eq!(
			layout(&short),
			vec![(1, 2, 2), (3, 4, 2), (5, 6, 2)],
			"one byte short of three records must fit only two"
		);
	}

	pub fn writer_not_blocked_by_requested_cut(combination: Combination) {
		// passing the cut size asks the flusher for a block; the writer must keep being accepted while that cut
		// is outstanding
		assert_unit_model();
		let (fixture, buffer) = fixture(combination, unit() * 3);
		let held = buffer.flush_guard();

		for version in 1..=30u64 {
			fixture.store.write(&record(version, 1)).expect("an outstanding cut must never reject a write");
			assert_eq!(
				fixture.store.read(CommitVersion(version)).unwrap().map(|cdc| cdc.version),
				Some(CommitVersion(version)),
				"a record must be readable the instant it is written"
			);
		}

		let metrics = fixture.store.commit_metrics();
		assert_eq!(metrics.stalls, 0, "a requested cut is not a stall; only the ceiling may stall a writer");
		assert_eq!(metrics.blocks_cut, 0, "the flusher is held back, so nothing can have been cut yet");
		assert_eq!(metrics.entries.as_u64(), 30, "every write must still be resident in the commit tier");
		assert!(layout(&fixture).is_empty());

		drop(held);

		// the flush interval is an hour away and no explicit flush has been issued, so a sealed block proves
		// the byte trigger fired on its own
		let deadline = Instant::now() + AUTO_CUT_TIMEOUT;
		while layout(&fixture).is_empty() && Instant::now() < deadline {
			sleep(StdDuration::from_millis(5));
		}
		assert!(
			!layout(&fixture).is_empty(),
			"passing the cut size must request a cut without an explicit flush"
		);

		assert!(fixture.store.flush_pending());
		assert_eq!(
			layout(&fixture),
			vec![
				(1, 3, 3),
				(4, 6, 3),
				(7, 9, 3),
				(10, 12, 3),
				(13, 15, 3),
				(16, 18, 3),
				(19, 21, 3),
				(22, 24, 3),
				(25, 27, 3),
				(28, 30, 3)
			]
		);
	}

	pub fn multi_block_cut_loses_no_record(combination: Combination) {
		// cutting a buffer into nine blocks must not lose, duplicate or reorder a record
		assert_unit_model();
		let (fixture, buffer) = fixture(combination, unit() * 3);

		write_then_cut(&fixture, &buffer, &[1; 25]);

		assert_eq!(
			layout(&fixture),
			vec![
				(1, 3, 3),
				(4, 6, 3),
				(7, 9, 3),
				(10, 12, 3),
				(13, 15, 3),
				(16, 18, 3),
				(19, 21, 3),
				(22, 24, 3),
				(25, 25, 1)
			]
		);

		let batch = fixture.store.read_range(Bound::Unbounded, Bound::Unbounded, 1024).unwrap();
		let versions: Vec<u64> = batch.items.iter().map(|cdc| cdc.version.0).collect();
		assert_eq!(
			versions,
			(1..=25).collect::<Vec<_>>(),
			"the range walk must return every version exactly once"
		);
		assert!(!batch.has_more);

		for version in 1..=25u64 {
			let cdc =
				fixture.store.read(CommitVersion(version)).unwrap().expect("every record must survive");
			assert_eq!(cdc.version, CommitVersion(version));
			assert_eq!(
				cdc.timestamp.to_nanos(),
				TIMESTAMP_BASE + version,
				"a record must keep its own payload"
			);
			assert_eq!(cdc.changes.len(), 1);
		}

		assert_eq!(fixture.store.min_version().unwrap(), Some(CommitVersion(1)));
		assert_eq!(fixture.store.max_version().unwrap(), Some(CommitVersion(25)));
	}

	pub fn every_cut_block_satisfies_append_invariants(combination: Combination) {
		// every block a cut produces must be one a reader can trust: ascending versions, a count matching its
		// payload, no overlap with the block before it
		assert_unit_model();
		let (fixture, buffer) = fixture(combination, unit() * 4);
		let sizes = [1, 1, 3, 1, 2, 4, 1, 9, 1];

		write_then_cut(&fixture, &buffer, &sizes);

		let summaries = fixture.persistent.summaries_from(CommitVersion(0), SUMMARY_LIMIT).unwrap();
		assert!(summaries.len() > 1, "this run must cut more than one block or it proves nothing");

		let mut seen: Vec<u64> = Vec::new();
		let mut previous_max: Option<CommitVersion> = None;
		for summary in &summaries {
			let block = fixture
				.persistent
				.load_block(summary.id)
				.unwrap()
				.expect("a summary must name a block");
			assert!(!block.entries.is_empty(), "an empty block has no version range");
			assert!(
				block.entries.windows(2).all(|w| w[0].version < w[1].version),
				"block entries must be strictly ascending by version"
			);
			assert_eq!(
				block.summary.id.0,
				block.entries.last().unwrap().version,
				"a block is identified by its highest version"
			);
			assert_eq!(block.summary.min_version, block.entries.first().unwrap().version);
			assert_eq!(block.summary.max_version, block.entries.last().unwrap().version);
			assert_eq!(
				block.summary.count.as_u64(),
				block.entries.len() as u64,
				"summary count must match the entries the payload carries"
			);
			assert_eq!(block.summary.count, summary.count);
			if let Some(previous) = previous_max {
				assert!(previous < block.min_version(), "blocks must not overlap");
			}
			previous_max = Some(block.max_version());
			seen.extend(block.entries.iter().map(|cdc| cdc.version.0));
		}

		assert_eq!(
			seen,
			(1..=sizes.len() as u64).collect::<Vec<_>>(),
			"the blocks together must be the whole run"
		);
	}

	pub fn no_block_exceeds_cut_bytes_unless_one_record_does(combination: Combination) {
		// the cut size bounds a block in bytes, not in records: a block may only pass it when it carries a
		// single record that already does
		assert_unit_model();
		let cut_bytes = unit() * 4;
		let (fixture, buffer) = fixture(combination, cut_bytes);

		write_then_cut(&fixture, &buffer, &[1, 1, 3, 1, 2, 4, 1, 9, 1]);

		let summaries = fixture.persistent.summaries_from(CommitVersion(0), SUMMARY_LIMIT).unwrap();
		let mut oversized = 0;
		for summary in &summaries {
			let block = fixture.persistent.load_block(summary.id).unwrap().unwrap();
			let bytes = block.resident_bytes();
			if bytes > cut_bytes {
				assert_eq!(
					block.entries.len(),
					1,
					"only a single record larger than the cut size may pass it, block {:?} holds {} at {bytes}",
					block.id(),
					block.entries.len()
				);
				oversized += 1;
			}
		}
		assert_eq!(oversized, 1, "exactly the nine-unit record must have formed an oversized block");
	}
}

crate::tier_tests!(
	[
		memory = memory,
		memory_cached = memory_cached,
		sqlite = sqlite,
		sqlite_cached = sqlite_cached,
		sqlite_starved_cache = sqlite_starved_cache,
	],
	[
		flush_emits_several_blocks,
		block_boundaries_follow_byte_accounting,
		oversized_record_forms_its_own_block,
		boundary_record_joins_the_block,
		writer_not_blocked_by_requested_cut,
		multi_block_cut_loses_no_record,
		every_cut_block_satisfies_append_invariants,
		no_block_exceeds_cut_bytes_unless_one_record_does,
	]
);
