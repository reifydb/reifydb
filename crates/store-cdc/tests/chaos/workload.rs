// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Writes, flushes, retention and boots interleave in one stream, putting reads on the seam where a record can be lost,
//! repeated or resurrected.

use std::{
	collections::{BTreeSet, Bound},
	ops::RangeInclusive,
};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
use reifydb_store_cdc::{
	error::CdcError,
	storage::{CdcStorage, Cutoff},
};
use reifydb_testing_chaos::fuzz::{pick, run_reported, split};
use reifydb_value::value::datetime::DateTime;

use crate::{
	fixtures::{ChangeKind, Config, Harness, flush, record},
	oracle::{Eviction, Record, TtlCutoff, Version},
};

#[derive(Clone, Debug)]
pub struct Params {
	pub min_steps: u32,
	pub max_steps: u32,
	pub write_pct: u32,
	pub flush_pct: u32,
	pub drop_pct: u32,
	pub unbounded_drop_pct: u32,
	pub reopen_pct: u32,
	pub duplicate_pct: u32,
	pub max_changes: usize,
	pub max_value_bytes: usize,
	pub max_gap: u64,
	pub tables: u64,
	pub rows: u64,
	pub max_batch: u64,
	pub max_limit: usize,
	pub timestamp_span: u64,
}

pub const TIMESTAMP_BASE: u64 = 1_700_000_000_000_000_000;

/// Versions only ever move up and no two records share a timestamp, so the earliest block at or after a ttl cutoff is
/// unambiguous.
pub struct Generator {
	next_version: u64,
	written: Vec<u64>,
	used: BTreeSet<u64>,
}

impl Generator {
	pub fn new() -> Self {
		Self {
			next_version: 0,
			written: Vec::new(),
			used: BTreeSet::new(),
		}
	}

	pub fn advance(&mut self, rng: &mut StdRng, p: &Params) -> u64 {
		self.next_version += rng.random_range(1..=p.max_gap);
		self.written.push(self.next_version);
		self.next_version
	}

	pub fn timestamp(&mut self, rng: &mut StdRng, p: &Params) -> u64 {
		loop {
			let candidate = TIMESTAMP_BASE + rng.random_range(0..p.timestamp_span);
			if self.used.insert(candidate) {
				return candidate;
			}
		}
	}

	/// Picks a version every configuration must refuse, chosen from the raw model state rather than the refusal
	/// predicate, so a predicate that stopped refusing is caught instead of hidden.
	pub fn duplicate(&self, rng: &mut StdRng, configs: &[Config]) -> Option<u64> {
		for _ in 0..8 {
			if self.written.is_empty() {
				return None;
			}
			let candidate = self.written[rng.random_range(0..self.written.len() as u32) as usize];
			let refused = configs.iter().all(|config| {
				config.oracle.sealed_contains(candidate) || config.oracle.live_contains(candidate)
			});
			if refused {
				return Some(candidate);
			}
		}
		None
	}

	pub fn ceiling(&self) -> u64 {
		self.next_version
	}
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut harness = Harness::new();
	let mut generator = Generator::new();

	let steps = rng.random_range(p.min_steps..=p.max_steps);
	for step in 0..steps {
		let roll = rng.random_range(0u32..100);
		let mut cut = p.write_pct;
		if roll < cut {
			write_step(&mut rng, &mut harness, &mut generator, &p, step);
		} else {
			cut += p.flush_pct;
			if roll < cut {
				flush_step(&mut harness, step);
			} else {
				cut += p.drop_pct;
				if roll < cut {
					drop_step(&mut rng, &mut harness, &generator, &p, step);
				} else {
					cut += p.reopen_pct;
					if roll < cut {
						reopen_step(&mut harness, step);
					} else {
						read_step(&mut rng, &harness, &generator, &p, step);
					}
				}
			}
		}
		for config in &mut harness.configs {
			config.oracle.check_invariants(config.name, step);
			check_bounds(config, step);
			check_commit_metrics(config, step);
			check_read_buffer(config, step);
		}
	}

	sweep(&mut harness, &p, steps);
}

/// Checks every version, bound and the block layout after a full drain and after a fresh boot, the only state where a
/// still-buffered record may legally disappear.
fn sweep(harness: &mut Harness, p: &Params, step: u32) {
	for config in &harness.configs {
		verify(config, p, step);
	}
	harness.flush_all();
	for config in &mut harness.configs {
		config.oracle.check_invariants(config.name, step);
		check_blocks(config, step);
		verify(config, p, step);
	}
	for config in &mut harness.configs {
		config.reopen();
		config.oracle.check_invariants(config.name, step);
		check_blocks(config, step);
		verify(config, p, step);
		// verify has just read every surviving version, so every block has been offered to a cold read buffer
		check_eviction_reached(config, step);
	}
}

/// Checks every logical surface at once against one configuration's model.
pub fn verify(config: &Config, p: &Params, step: u32) {
	check_bounds(config, step);
	check_blocks(config, step);
	check_dropped_unreadable(config, step);
	for version in config.oracle.versions() {
		check_read(config, version, step);
		check_count(config, version, step);
	}
	check_drain(config, Bound::Unbounded, Bound::Unbounded, 1, step);
	check_drain(config, Bound::Unbounded, Bound::Unbounded, p.max_batch.max(2), step);
	check_range(config, Bound::Unbounded, Bound::Unbounded, u64::from(u32::MAX), step);
	check_range(config, Bound::Unbounded, Bound::Unbounded, 0, step);
	for offset in 0..4u64 {
		let cutoff = TIMESTAMP_BASE + offset * (p.timestamp_span / 4).max(1);
		check_ttl(config, cutoff, step);
	}
	check_ttl(config, 0, step);
	// A cutoff at or above 2^63 wraps negative in the sqlite tier's i64 comparison, so the probe stops below it.
	check_ttl(config, i64::MAX as u64, step);
}

fn write_step(rng: &mut StdRng, harness: &mut Harness, generator: &mut Generator, p: &Params, step: u32) {
	if rng.random_range(0u32..100) < p.duplicate_pct {
		if let Some(version) = generator.duplicate(rng, &harness.configs) {
			duplicate_write(rng, harness, generator, p, version, step);
			return;
		}
	}
	let (version, cdc, row) = build_record(rng, generator, p);

	for config in &mut harness.configs {
		assert!(
			config.oracle.write(version, row.clone()),
			"WRITE rejected by the model: config={} step={step} version={version}",
			config.name
		);
		config.store.write(&cdc).unwrap_or_else(|err| {
			panic!(
				"WRITE rejected by the store: config={} step={step} version={version} err={err:?}",
				config.name
			)
		});
		if config.oracle.should_cut() {
			flush(config);
			check_blocks(config, step);
		}
	}
}

/// A duplicate version must be refused and must leave the log exactly as it was; a silent overwrite would rewrite a
/// block a reader may already be holding.
fn duplicate_write(
	rng: &mut StdRng,
	harness: &mut Harness,
	generator: &mut Generator,
	p: &Params,
	version: u64,
	step: u32,
) {
	let timestamp = generator.timestamp(rng, p);
	let (cdc, row) = build(rng, p, version, timestamp);

	for config in &mut harness.configs {
		let before = fingerprint(config);
		let expected = config.oracle.read(version).cloned();
		assert!(
			!config.oracle.write(version, row.clone()),
			"DUPLICATE accepted by the model: config={} step={step} version={version}",
			config.name
		);
		match config.store.write(&cdc) {
			Err(CdcError::DuplicateVersion(reported)) => assert_eq!(
				reported,
				CommitVersion(version),
				"DUPLICATE reported the wrong version: config={} step={step} version={version}",
				config.name
			),
			other => panic!(
				"DUPLICATE was not refused: config={} step={step} version={version} got={other:?}",
				config.name
			),
		}
		assert_eq!(
			fingerprint(config),
			before,
			"DUPLICATE mutated the log: config={} step={step} version={version}",
			config.name
		);
		if let Some(expected) = expected {
			let got = config.store.read(CommitVersion(version)).unwrap();
			let got = got.expect("a refused duplicate must leave the original readable");
			assert_eq!(
				(got.changes, got.timestamp.to_nanos()),
				(expected.changes, expected.timestamp),
				"DUPLICATE overwrote the record it collided with: config={} step={step} version={version}",
				config.name
			);
		}
	}
}

fn flush_step(harness: &mut Harness, step: u32) {
	for config in &mut harness.configs {
		flush(config);
		check_blocks(config, step);
	}
}

fn drop_step(rng: &mut StdRng, harness: &mut Harness, generator: &Generator, p: &Params, step: u32) {
	// an unbounded reach clears the read buffer instead of invalidating a prefix, a path a version cutoff never
	// takes
	let cutoff = if rng.random_range(0u32..100) < p.unbounded_drop_pct {
		TtlCutoff::Unbounded
	} else {
		TtlCutoff::Version(rng.random_range(0..=generator.ceiling().saturating_add(2)))
	};
	let limit = rng.random_range(0..=p.max_limit as u64) as usize;
	for config in &mut harness.configs {
		let expected = config.oracle.drop_before(cutoff, limit);
		let got = config.store.drop_before(store_cutoff(cutoff), limit).unwrap();
		let actual = Eviction {
			count: got.count.as_u64(),
			sources: got.entries.len(),
			key_bytes: got.entries.iter().map(|entry| entry.key_bytes.as_bytes()).sum(),
			value_bytes: got.entries.iter().map(|entry| entry.value_bytes.as_bytes()).sum(),
			more_remaining: got.more_remaining,
		};
		assert_eq!(
			actual, expected,
			"DROP mismatch: config={} step={step} cutoff={cutoff:?} limit={limit} store={actual:?} oracle={expected:?}",
			config.name
		);
		let reported: u64 = got.entries.iter().map(|entry| entry.count.as_u64()).sum();
		assert_eq!(
			reported,
			got.count.as_u64(),
			"DROP rollup does not add up to the reported count: config={} step={step} cutoff={cutoff:?}",
			config.name
		);
		check_blocks(config, step);
		check_dropped_unreadable(config, step);
	}
}

fn reopen_step(harness: &mut Harness, step: u32) {
	for config in &mut harness.configs {
		config.reopen();
		check_blocks(config, step);
	}
}

fn read_step(rng: &mut StdRng, harness: &Harness, generator: &Generator, p: &Params, step: u32) {
	match rng.random_range(0u32..8) {
		0 | 1 => {
			let version = random_version(rng, generator);
			for config in &harness.configs {
				check_read(config, version, step);
			}
		}
		2 => {
			let version = random_version(rng, generator);
			for config in &harness.configs {
				check_count(config, version, step);
			}
		}
		3 | 4 | 5 => {
			let (start, end) = random_bounds(rng, generator);
			let batch = rng.random_range(0..=p.max_batch);
			for config in &harness.configs {
				check_range(config, start, end, batch, step);
			}
		}
		6 => {
			let (start, end) = random_bounds(rng, generator);
			let batch = rng.random_range(1..=p.max_batch.max(1));
			for config in &harness.configs {
				check_drain(config, start, end, batch, step);
			}
		}
		_ => {
			let cutoff = TIMESTAMP_BASE + rng.random_range(0..=p.timestamp_span.saturating_add(2));
			for config in &harness.configs {
				check_ttl(config, cutoff, step);
			}
		}
	}
}

pub fn check_read(config: &Config, version: Version, step: u32) {
	let expected = config.oracle.read(version);
	let got = config.store.read(CommitVersion(version)).unwrap();
	match (got, expected) {
		(Some(cdc), Some(record)) => {
			assert_eq!(
				cdc.version,
				CommitVersion(version),
				"READ answered for the wrong version: config={} step={step} version={version}",
				config.name
			);
			// the whole change list, so a payload that survived the wrong codec cannot pass on count alone
			assert_eq!(
				(&cdc.changes, cdc.timestamp.to_nanos()),
				(&record.changes, record.timestamp),
				"READ mismatch: config={} step={step} version={version}",
				config.name
			);
		}
		(None, None) => {}
		(got, expected) => panic!(
			"READ mismatch: config={} step={step} version={version} store={:?} oracle={:?}",
			config.name,
			got.map(|cdc| (cdc.changes.len(), cdc.timestamp.to_nanos())),
			expected.map(|record| (record.changes.len(), record.timestamp))
		),
	}
}

pub fn check_count(config: &Config, version: Version, step: u32) {
	let expected = config.oracle.read(version).map(|record| record.changes.len()).unwrap_or(0);
	let got = config.store.count(CommitVersion(version)).unwrap();
	assert_eq!(
		got, expected,
		"COUNT mismatch: config={} step={step} version={version} store={got} oracle={expected}",
		config.name
	);
}

/// `has_more` is exactly whether a record above the last returned version is still in range: a spurious claim costs a
/// consumer an empty poll every page, and a denied one strands the tail forever.
pub fn check_range(config: &Config, start: Bound<CommitVersion>, end: Bound<CommitVersion>, batch: u64, step: u32) {
	let got = config.store.read_range(start, end, batch).unwrap();
	let versions: Vec<u64> = got.items.iter().map(|cdc| cdc.version.0).collect();

	let Some((lo, hi)) = normalize(start, end) else {
		assert!(
			versions.is_empty() && !got.has_more,
			"RANGE over an empty bound pair answered {versions:?} has_more={}: config={} step={step}",
			got.has_more,
			config.name
		);
		return;
	};
	if batch == 0 {
		assert!(
			versions.is_empty(),
			"RANGE with a zero batch returned rows: config={} step={step} rows={versions:?}",
			config.name
		);
		// a zero batch asks only whether the range holds anything, so a hole between two records must answer no
		assert_eq!(
			got.has_more,
			config.oracle.has_in(lo, hi),
			"RANGE with a zero batch misreported the range as non-empty: config={} step={step} lo={lo} hi={hi} store={} oracle={}",
			config.name,
			got.has_more,
			config.oracle.has_in(lo, hi)
		);
		return;
	}

	let expected = config.oracle.range(lo, hi, batch as usize);
	assert_eq!(
		versions, expected,
		"RANGE mismatch: config={} step={step} lo={lo} hi={hi} batch={batch} store={versions:?} oracle={expected:?}",
		config.name
	);
	assert_content(config, &got.items, step);

	match versions.last() {
		Some(last) => {
			if got.has_more {
				assert_eq!(
					versions.len() as u64,
					batch,
					"RANGE claimed more without filling the batch: config={} step={step} lo={lo} hi={hi} batch={batch}",
					config.name
				);
			}
			assert_eq!(
				got.has_more,
				config.oracle.has_above(*last, hi),
				"RANGE misreported the continuation: config={} step={step} lo={lo} hi={hi} batch={batch} last={last} store={} oracle={}",
				config.name,
				got.has_more,
				config.oracle.has_above(*last, hi)
			);
		}
		None => assert!(
			!got.has_more && !config.oracle.has_in(lo, hi),
			"RANGE returned nothing over a non-empty range: config={} step={step} lo={lo} hi={hi} batch={batch} has_more={}",
			config.name,
			got.has_more
		),
	}
}

/// A page boundary is where the merge walk hands off between a sealed block, a truncated hole and the commit tier, so a
/// lost or repeated record there is invisible to a single-page read.
pub fn check_drain(config: &Config, start: Bound<CommitVersion>, end: Bound<CommitVersion>, batch: u64, step: u32) {
	let Some((lo, hi)) = normalize(start, end) else {
		return;
	};
	let expected = config.oracle.range(lo, hi, usize::MAX);
	let mut cursor = start;
	let mut drained: Vec<u64> = Vec::new();
	let mut pulls = 0usize;
	loop {
		let page = config.store.read_range(cursor, end, batch).unwrap();
		assert_content(config, &page.items, step);
		for cdc in &page.items {
			drained.push(cdc.version.0);
		}
		match page.items.last() {
			Some(cdc) => cursor = Bound::Excluded(cdc.version),
			None => break,
		}
		if !page.has_more {
			break;
		}
		pulls += 1;
		assert!(
			pulls <= expected.len() + 2,
			"DRAIN did not terminate: config={} step={step} lo={lo} hi={hi} batch={batch} after {pulls} pulls",
			config.name
		);
	}
	assert_eq!(
		drained,
		expected,
		"DRAIN mismatch: config={} step={step} lo={lo} hi={hi} batch={batch} store={} rows oracle={} rows",
		config.name,
		drained.len(),
		expected.len()
	);
}

pub fn check_ttl(config: &Config, cutoff: u64, step: u32) {
	let expected = config.oracle.find_ttl_cutoff(cutoff);
	let got = config.store.find_ttl_cutoff(nanos(cutoff)).unwrap().map(|reach| match reach {
		Cutoff::Version(version) => TtlCutoff::Version(version.0),
		Cutoff::Unbounded => TtlCutoff::Unbounded,
	});
	assert_eq!(
		got, expected,
		"TTL_CUTOFF mismatch: config={} step={step} cutoff={cutoff} store={got:?} oracle={expected:?}",
		config.name
	);
}

pub fn check_bounds(config: &Config, step: u32) {
	let min = config.store.min_version().unwrap().map(|version| version.0);
	let max = config.store.max_version().unwrap().map(|version| version.0);
	let floor = config.store.truncated_before().unwrap().0;
	assert_eq!(
		min,
		config.oracle.min_version(),
		"MIN_VERSION mismatch: config={} step={step} store={min:?} oracle={:?}",
		config.name,
		config.oracle.min_version()
	);
	assert_eq!(
		max,
		config.oracle.max_version(),
		"MAX_VERSION mismatch: config={} step={step} store={max:?} oracle={:?}",
		config.name,
		config.oracle.max_version()
	);
	assert_eq!(
		floor,
		config.oracle.truncated_before(),
		"TRUNCATED_BEFORE mismatch: config={} step={step} store={floor} oracle={}",
		config.name,
		config.oracle.truncated_before()
	);
	assert!(
		floor >= config.floor_seen,
		"TRUNCATED_BEFORE moved backwards: config={} step={step} was {} now {floor}",
		config.name,
		config.floor_seen
	);
}

/// The sealed block layout must be ascending, disjoint, gapless over the versions it claims, and identical to what the
/// model cut.
pub fn check_blocks(config: &Config, step: u32) {
	let got = config.summaries();
	let expected: Vec<(u64, u64, u64, u64, u64)> = config
		.oracle
		.blocks()
		.iter()
		.map(|block| (block.min, block.max, block.count, block.min_timestamp, block.max_timestamp))
		.collect();
	assert_eq!(
		got, expected,
		"BLOCKS mismatch: config={} step={step} store={got:?} oracle={expected:?}",
		config.name
	);

	let mut previous: Option<(u64, u64)> = None;
	for (min, max, count, _, _) in &got {
		assert!(
			min <= max,
			"BLOCKS carry an inverted range: config={} step={step} [{min}..{max}]",
			config.name
		);
		assert!(*count > 0, "BLOCKS carry an empty block: config={} step={step} [{min}..{max}]", config.name);
		if let Some((_, previous_max)) = previous {
			assert!(
				previous_max < *min,
				"BLOCKS overlap: config={} step={step} [..{previous_max}] then [{min}..]",
				config.name
			);
		}
		previous = Some((*min, *max));
	}
}

pub fn check_dropped_unreadable(config: &Config, step: u32) {
	let floor = config.oracle.truncated_before();
	for version in config.oracle.dropped() {
		assert!(
			config.store.read(CommitVersion(*version)).unwrap().is_none(),
			"a dropped version is still readable: config={} step={step} version={version} floor={floor}",
			config.name
		);
	}
	if floor > 0 {
		let page =
			config.store.read_range(Bound::Unbounded, Bound::Excluded(CommitVersion(floor)), 64).unwrap();
		let leaked: Vec<u64> = page.items.iter().map(|cdc| cdc.version.0).collect();
		assert!(
			leaked.is_empty(),
			"a range below the floor returned rows: config={} step={step} floor={floor} rows={leaked:?}",
			config.name
		);
	}
}

/// A block cut the harness did not ask for would move a boundary the model cannot see, breaking the run's dependence on
/// the seed alone.
pub fn check_commit_metrics(config: &Config, step: u32) {
	let metrics = config.store.commit_metrics();
	assert_eq!(
		metrics.entries.as_u64(),
		config.oracle.live_len() as u64,
		"COMMIT entries mismatch: config={} step={step}",
		config.name
	);
	assert_eq!(
		metrics.resident_bytes.as_bytes(),
		config.oracle.live_bytes(),
		"COMMIT bytes mismatch: config={} step={step}",
		config.name
	);
	assert_eq!(
		metrics.blocks_cut,
		config.oracle.blocks_cut(),
		"COMMIT cut a block the harness never asked for: config={} step={step}, so the block layout is no longer a function of the seed",
		config.name
	);
	assert_eq!(
		metrics.stalls, 0,
		"COMMIT stalled against its ceiling: config={} step={step}, so the run no longer depends only on the seed",
		config.name
	);
}

fn assert_content(config: &Config, items: &[Cdc], step: u32) {
	for cdc in items {
		let record = config.oracle.read(cdc.version.0).unwrap_or_else(|| {
			panic!(
				"RANGE returned a version the model does not hold: config={} step={step} version={}",
				config.name, cdc.version.0
			)
		});
		assert_eq!(
			(&cdc.changes, cdc.timestamp.to_nanos()),
			(&record.changes, record.timestamp),
			"RANGE returned the wrong record: config={} step={step} version={}",
			config.name,
			cdc.version.0
		);
	}
}

/// The budget accounting behind the LRU: bytes charged on insert must come back on eviction and on invalidation, or a
/// buffer that reports itself full while holding nothing quietly stops caching for the rest of the run.
pub fn check_read_buffer(config: &Config, step: u32) {
	for shard in config.store.read_buffer_shard_metrics() {
		assert!(
			shard.used <= shard.limit,
			"READ BUFFER is over budget: config={} step={step} shard={} used={} limit={}",
			config.name,
			shard.shard,
			shard.used.as_bytes(),
			shard.limit.as_bytes()
		);
		assert!(
			shard.counters.evictions <= shard.counters.insertions,
			"READ BUFFER evicted more blocks than it ever held: config={} step={step} shard={} evictions={} insertions={}",
			config.name,
			shard.shard,
			shard.counters.evictions,
			shard.counters.insertions
		);
		if shard.blocks == 0 {
			assert_eq!(
				shard.used.as_bytes(),
				0,
				"READ BUFFER holds no block yet still charges bytes: config={} step={step} shard={} used={}",
				config.name,
				shard.shard,
				shard.used.as_bytes()
			);
		}
	}
}

/// A budget smaller than the sealed log cannot have kept every block, so a run that never evicted never picked a victim
/// and the LRU went untested.
pub fn check_eviction_reached(config: &Config, step: u32) {
	let shards = config.store.read_buffer_shard_metrics();
	if shards.is_empty() {
		return;
	}
	let limit: u64 = shards.iter().map(|shard| shard.limit.as_bytes()).sum();
	let evictions: u64 = shards.iter().map(|shard| shard.counters.evictions).sum();
	if config.oracle.sealed_bytes() <= limit {
		return;
	}
	assert!(
		evictions > 0,
		"READ BUFFER never evicted while the sealed log outgrew it: config={} step={step} sealed={} limit={limit}",
		config.name,
		config.oracle.sealed_bytes()
	);
}

fn store_cutoff(cutoff: TtlCutoff) -> Cutoff {
	match cutoff {
		TtlCutoff::Version(version) => Cutoff::Version(CommitVersion(version)),
		TtlCutoff::Unbounded => Cutoff::Unbounded,
	}
}

/// A fresh version above everything ever written, with a timestamp no other record carries.
pub fn build_record(rng: &mut StdRng, generator: &mut Generator, p: &Params) -> (u64, Cdc, Record) {
	let version = generator.advance(rng, p);
	let timestamp = generator.timestamp(rng, p);
	let (cdc, row) = build(rng, p, version, timestamp);
	(version, cdc, row)
}

fn build(rng: &mut StdRng, p: &Params, version: u64, timestamp: u64) -> (Cdc, Record) {
	let count = rng.random_range(1..=p.max_changes as u32) as usize;
	let changes: Vec<(u64, u64, usize, ChangeKind)> = (0..count)
		.map(|_| {
			(
				rng.random_range(1..=p.tables),
				rng.random_range(1..=p.rows),
				rng.random_range(1..=p.max_value_bytes as u32) as usize,
				change_kind(rng),
			)
		})
		.collect();
	record(version, timestamp, &changes)
}

/// A bare delete charges zero value bytes, the one shape that can hide a rollup summing the wrong field.
fn change_kind(rng: &mut StdRng) -> ChangeKind {
	match rng.random_range(0u32..10) {
		0..=4 => ChangeKind::Insert,
		5..=7 => ChangeKind::Update,
		8 => ChangeKind::Delete {
			pre: true,
			visible: true,
		},
		_ => ChangeKind::Delete {
			pre: false,
			visible: false,
		},
	}
}

fn fingerprint(config: &Config) -> (Option<u64>, Option<u64>, u64, u64, u64) {
	(
		config.store.min_version().unwrap().map(|version| version.0),
		config.store.max_version().unwrap().map(|version| version.0),
		config.store.truncated_before().unwrap().0,
		config.store.commit_metrics().entries.as_u64(),
		config.store.commit_metrics().resident_bytes.as_bytes(),
	)
}

fn nanos(value: u64) -> DateTime {
	DateTime::from_nanos(value)
}

fn random_version(rng: &mut StdRng, generator: &Generator) -> u64 {
	rng.random_range(0..=generator.ceiling().saturating_add(3))
}

fn random_bounds(rng: &mut StdRng, generator: &Generator) -> (Bound<CommitVersion>, Bound<CommitVersion>) {
	let span: RangeInclusive<u64> = 0..=generator.ceiling().saturating_add(3);
	let a = rng.random_range(span.clone());
	let b = rng.random_range(span);
	let (low, high) = if a <= b {
		(a, b)
	} else {
		(b, a)
	};
	let start = match rng.random_range(0u32..3) {
		0 => Bound::Included(CommitVersion(low)),
		1 => Bound::Excluded(CommitVersion(low)),
		_ => Bound::Unbounded,
	};
	let end = match rng.random_range(0u32..3) {
		0 => Bound::Included(CommitVersion(high)),
		1 => Bound::Excluded(CommitVersion(high)),
		_ => Bound::Unbounded,
	};
	(start, end)
}

/// The bound contract the log answers on, restated here so the model owns it rather than echoing the store.
fn normalize(start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Option<(u64, u64)> {
	let lo = match start {
		Bound::Included(version) => version.0,
		Bound::Excluded(version) => version.0.saturating_add(1),
		Bound::Unbounded => 0,
	};
	let hi = match end {
		Bound::Included(version) => version.0,
		Bound::Excluded(version) => version.0.saturating_sub(1),
		Bound::Unbounded => u64::MAX,
	};
	if lo > hi {
		None
	} else {
		Some((lo, hi))
	}
}

/// Explores the parameter space, so a failure reports the RESOLVED parameters rather than the master seed.
pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let min_steps = rng.random_range(90..=200u32);
	let params = Params {
		min_steps,
		max_steps: min_steps + rng.random_range(40..=120u32),
		write_pct: rng.random_range(30..=55u32),
		flush_pct: rng.random_range(5..=25u32),
		drop_pct: rng.random_range(3..=14u32),
		unbounded_drop_pct: rng.random_range(0..=25u32),
		reopen_pct: rng.random_range(0..=8u32),
		duplicate_pct: rng.random_range(5..=30u32),
		max_changes: pick(&mut rng, &[1usize, 2, 4]),
		max_value_bytes: pick(&mut rng, &[1usize, 8, 32]),
		max_gap: pick(&mut rng, &[1u64, 2, 5]),
		tables: rng.random_range(1..=4u64),
		rows: rng.random_range(1..=16u64),
		max_batch: pick(&mut rng, &[1u64, 2, 5, 16]),
		max_limit: pick(&mut rng, &[0usize, 1, 3, 8]),
		timestamp_span: pick(&mut rng, &[1_000u64, 1_000_000]),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("cdc_store_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
