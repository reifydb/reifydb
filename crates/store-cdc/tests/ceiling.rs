// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::Bound,
	sync::{
		Arc, Barrier,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration as StdDuration, Instant},
};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_runtime::sync::waiter::WaiterHandle;
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_cdc::{
	error::CdcError,
	storage::CdcStorage,
	store::CdcStore,
	tier::{
		commit::{CdcCommitBufferTier, CdcCommitMetrics},
		persistent::CdcPersistentTier,
		read::CdcReadConfig,
	},
	types::cdc_resident_bytes,
};
use reifydb_value::{
	byte_size::ByteSize,
	count::Count,
	util::cowvec::CowVec,
	value::{datetime::DateTime, duration::Duration},
};

mod common;

const PAYLOAD_BYTES: usize = 1000;

const CUT_RECORDS: u64 = 4;

const CEILING_RECORDS: u64 = 8;

const UNREACHABLE_CEILING_RECORDS: u64 = 4096;

const CONTESTED_VERSION: u64 = 50;

const WATCHDOG: Duration = Duration::from_seconds_const(120);

const JOIN: Duration = Duration::from_seconds_const(30);

const CONDITION: StdDuration = StdDuration::from_secs(30);

fn record(version: u64) -> Cdc {
	// every record must cost exactly the same, otherwise a ceiling in records is not a ceiling in bytes
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000 + version),
		vec![CdcChange::Insert {
			key: EncodedKey::new(version.to_be_bytes().to_vec()),
			post: EncodedBytes(CowVec::new(vec![7u8; PAYLOAD_BYTES])),
		}],
	)
}

fn budget(records: u64) -> ByteSize {
	ByteSize::from_bytes(cdc_resident_bytes(&record(1)).as_bytes() * records)
}

struct Watchdog {
	done: Arc<WaiterHandle>,
}

impl Watchdog {
	fn arm(label: &'static str) -> Self {
		// a deadlock must fail the run, never park the harness until CI kills it
		let done = Arc::new(WaiterHandle::new());
		let signal = Arc::clone(&done);
		thread::spawn(move || {
			if !signal.wait_timeout(WATCHDOG) {
				eprintln!("ceiling: {label} never finished, a stall did not end");
				std::process::exit(101);
			}
		});
		Self {
			done,
		}
	}
}

impl Drop for Watchdog {
	fn drop(&mut self) {
		self.done.notify();
	}
}

struct Signal {
	waiter: Arc<WaiterHandle>,
	returned: Arc<AtomicBool>,
}

impl Drop for Signal {
	fn drop(&mut self) {
		// fires on unwind too, otherwise a panicking writer reads as a deadlock
		self.returned.store(true, Ordering::SeqCst);
		self.waiter.notify();
	}
}

struct Worker<T> {
	handle: JoinHandle<T>,
	waiter: Arc<WaiterHandle>,
	returned: Arc<AtomicBool>,
}

impl<T> Worker<T> {
	fn returned(&self) -> bool {
		self.returned.load(Ordering::SeqCst)
	}

	fn join(self, label: &str) -> T {
		assert!(self.waiter.wait_timeout(JOIN), "{label}: a writer never came back from its ceiling stall");
		self.handle.join().expect("writer thread panicked")
	}
}

fn spawn<T, F>(body: F) -> Worker<T>
where
	T: Send + 'static,
	F: FnOnce() -> T + Send + 'static,
{
	let waiter = Arc::new(WaiterHandle::new());
	let returned = Arc::new(AtomicBool::new(false));
	let signal = Signal {
		waiter: Arc::clone(&waiter),
		returned: Arc::clone(&returned),
	};
	let handle = thread::spawn(move || {
		let _signal = signal;
		body()
	});
	Worker {
		handle,
		waiter,
		returned,
	}
}

fn wait_for(label: &str, mut ready: impl FnMut() -> bool) {
	// spins rather than sleeps so no ordering depends on a duration, and gives up so a lost wakeup fails
	let deadline = Instant::now() + CONDITION;
	while !ready() {
		assert!(Instant::now() < deadline, "{label}");
		thread::yield_now();
	}
}

struct Case {
	name: &'static str,
	store: CdcStore,
	buffer: CdcCommitBufferTier,
	persistent: CdcPersistentTier,
	_guard: Option<SqliteTempPathGuard>,
}

impl Case {
	fn metrics(&self) -> CdcCommitMetrics {
		self.buffer.metrics()
	}

	fn stalls(&self) -> u64 {
		self.buffer.metrics().stalls
	}

	fn write(&self, version: u64) {
		self.store
			.write(&record(version))
			.unwrap_or_else(|e| panic!("{}: v{version} was refused: {e}", self.name));
	}

	fn write_all(&self, versions: impl IntoIterator<Item = u64>) {
		for version in versions {
			self.write(version);
		}
	}

	fn flush(&self) {
		assert!(self.store.flush_pending(), "{}: the flusher did not answer", self.name);
	}

	fn versions(&self) -> Vec<u64> {
		let batch = self.store.read_range(Bound::Unbounded, Bound::Unbounded, 1_000_000).unwrap();
		assert!(!batch.has_more, "{}: the whole log must fit one batch", self.name);
		batch.items.iter().map(|cdc| cdc.version.0).collect()
	}

	fn assert_drained(&self) {
		let metrics = self.metrics();
		assert_eq!(metrics.entries, Count::ZERO, "{}: the commit tier still holds records", self.name);
		assert_eq!(
			metrics.resident_bytes,
			ByteSize::ZERO,
			"{}: an empty commit tier still charges bytes",
			self.name
		);
	}

	fn assert_blocks_partition_versions(&self) {
		// overlapping blocks make one version answerable from two payloads and misdirect prefix truncation
		let summaries = self.persistent.summaries_from(CommitVersion(0), 1_000_000).unwrap();
		for pair in summaries.windows(2) {
			assert!(
				pair[0].max_version < pair[1].min_version,
				"{}: block [{}..{}] overlaps block [{}..{}]",
				self.name,
				pair[0].min_version.0,
				pair[0].max_version.0,
				pair[1].min_version.0,
				pair[1].max_version.0
			);
		}
	}
}

fn for_each_tier(cut: ByteSize, ceiling: ByteSize, run: impl Fn(&Case)) {
	// the ceiling is the commit tier's alone, so a slower tier may change when a stall ends but never whether
	for name in ["memory", "memory_cached", "sqlite", "sqlite_cached"] {
		let read = name.ends_with("cached").then(CdcReadConfig::default);
		let (persistent, guard) = if name.starts_with("sqlite") {
			let (config, guard) = SqliteConfig::in_memory();
			(CdcPersistentTier::sqlite(config), Some(guard))
		} else {
			(CdcPersistentTier::memory(), None)
		};
		let commit = common::commit_config(cut, ceiling);
		let buffer = commit.storage.clone();
		let fixture = common::custom(persistent, read, commit, guard);
		run(&Case {
			name,
			store: fixture.store,
			buffer,
			persistent: fixture.persistent,
			_guard: fixture.guard,
		});
	}
}

#[test]
fn crossing_cut_bytes_never_parks_the_writer() {
	// cut_bytes only asks for a block: parking there would make every commit past it pay flusher latency
	let _watchdog = Watchdog::arm("crossing_cut_bytes_never_parks_the_writer");
	for_each_tier(budget(CUT_RECORDS), budget(UNREACHABLE_CEILING_RECORDS), |case| {
		let total = 500;
		case.write_all(1..=total);
		assert_eq!(case.stalls(), 0, "{}: a writer parked below the ceiling", case.name);

		case.flush();
		assert_eq!(case.stalls(), 0, "{}: a writer parked below the ceiling", case.name);
		assert!(
			case.metrics().blocks_cut >= total / CUT_RECORDS,
			"{}: {} records at {} per block cut only {} blocks",
			case.name,
			total,
			CUT_RECORDS,
			case.metrics().blocks_cut
		);
		case.assert_drained();
		assert_eq!(case.versions(), (1..=total).collect::<Vec<_>>());
	});
}

#[test]
fn the_ceiling_parks_the_writer_until_the_flusher_drains() {
	// the counter must prove the stall: from outside, a slow flusher and a parked writer look identical
	let _watchdog = Watchdog::arm("the_ceiling_parks_the_writer_until_the_flusher_drains");
	for_each_tier(budget(CUT_RECORDS), budget(CEILING_RECORDS), |case| {
		let frozen = case.buffer.flush_guard();
		case.write_all(1..=CEILING_RECORDS + 1);
		assert_eq!(case.stalls(), 0, "{}: filling up to the ceiling must not park anyone", case.name);

		let store = case.store.clone();
		let writer = spawn(move || {
			for version in CEILING_RECORDS + 2..=CEILING_RECORDS + 32 {
				store.write(&record(version)).unwrap();
			}
		});
		wait_for(&format!("{}: a write over the ceiling never stalled", case.name), || case.stalls() >= 1);

		assert!(!writer.returned(), "{}: a write completed while the flusher was frozen", case.name);
		assert!(
			case.buffer.resident_bytes() > budget(CEILING_RECORDS),
			"{}: the buffer parked below its ceiling",
			case.name
		);

		drop(frozen);
		writer.join(case.name);

		assert!(case.stalls() >= 1, "{}: the stall counter lost the stall it reported", case.name);
		case.flush();
		case.assert_drained();
		assert_eq!(case.versions(), (1..=CEILING_RECORDS + 32).collect::<Vec<_>>());
	});
}

#[test]
fn every_ceiling_stall_ends() {
	// a lost wakeup is not a wrong answer but a writer that never returns, so every round needs a bounded join
	let _watchdog = Watchdog::arm("every_ceiling_stall_ends");
	let rounds: u64 = 25;
	let per_round: u64 = 40;
	for_each_tier(budget(CUT_RECORDS), budget(CEILING_RECORDS), |case| {
		for round in 0..rounds {
			let base = round * per_round;
			case.flush();
			case.assert_drained();

			let before = case.stalls();
			let frozen = case.buffer.flush_guard();
			case.write_all(base + 1..=base + CEILING_RECORDS + 1);
			assert_eq!(case.stalls(), before, "{}: round {round} parked while filling", case.name);

			let store = case.store.clone();
			let writer = spawn(move || {
				for version in base + CEILING_RECORDS + 2..=base + per_round {
					store.write(&record(version)).unwrap();
				}
			});
			wait_for(&format!("{}: round {round} never stalled", case.name), || case.stalls() > before);
			drop(frozen);
			writer.join(&format!("{} round {round}", case.name));
		}

		assert!(
			case.stalls() >= rounds,
			"{}: {rounds} rounds over the ceiling counted only {} stalls",
			case.name,
			case.stalls()
		);
		case.flush();
		assert_eq!(case.versions(), (1..=rounds * per_round).collect::<Vec<_>>());
	});
}

#[test]
fn a_parked_writer_never_blocks_a_reader() {
	// a stall that kept the buffer lock instead of releasing it would hang every one of these reads
	let _watchdog = Watchdog::arm("a_parked_writer_never_blocks_a_reader");
	for_each_tier(budget(CUT_RECORDS), budget(CEILING_RECORDS), |case| {
		case.write_all(1..=20);
		case.flush();

		let frozen = case.buffer.flush_guard();
		case.write_all(21..=29);
		// the fill above may have parked the main thread already, so only a new stall proves this writer parked
		let before = case.stalls();
		let store = case.store.clone();
		let writer = spawn(move || {
			for version in 30..=36 {
				store.write(&record(version)).unwrap();
			}
		});
		wait_for(&format!("{}: a write over the ceiling never stalled", case.name), || case.stalls() > before);
		assert!(!writer.returned(), "{}: the writer was never parked", case.name);

		let sealed = case.store.read(CommitVersion(1)).unwrap();
		assert_eq!(sealed.map(|cdc| cdc.version), Some(CommitVersion(1)), "{}: sealed read", case.name);
		let live = case.store.read(CommitVersion(25)).unwrap();
		assert_eq!(live.map(|cdc| cdc.version), Some(CommitVersion(25)), "{}: commit tier read", case.name);
		assert_eq!(case.versions(), (1..=29).collect::<Vec<_>>(), "{}: range read", case.name);
		assert_eq!(case.store.min_version().unwrap(), Some(CommitVersion(1)), "{}: min_version", case.name);
		assert_eq!(case.store.max_version().unwrap(), Some(CommitVersion(29)), "{}: max_version", case.name);
		assert!(!writer.returned(), "{}: a read released the parked writer", case.name);

		drop(frozen);
		writer.join(case.name);
		case.flush();
		assert_eq!(case.versions(), (1..=36).collect::<Vec<_>>());
	});
}

#[test]
fn a_completed_flush_does_not_suppress_the_next_one() {
	// with the interval timer an hour out, a request the triggered flag swallows leaves records in forever
	let _watchdog = Watchdog::arm("a_completed_flush_does_not_suppress_the_next_one");
	let rounds: u64 = 4;
	let per_round: u64 = 8;
	for_each_tier(budget(CUT_RECORDS), budget(UNREACHABLE_CEILING_RECORDS), |case| {
		for round in 0..rounds {
			let base = round * per_round;
			let before = case.metrics().blocks_cut;
			case.write_all(base + 1..=base + per_round);
			// a remainder under cut_bytes may wait for the interval, a buffer at or over it may not
			wait_for(&format!("{}: round {round} sat over cut_bytes forever", case.name), || {
				case.metrics().entries < Count::new(CUT_RECORDS)
			});
			assert!(
				case.metrics().blocks_cut > before,
				"{}: round {round} cut no block of its own",
				case.name
			);
		}

		assert_eq!(case.stalls(), 0, "{}: nothing may park below the ceiling", case.name);
		case.flush();
		case.assert_drained();
		assert_eq!(case.versions(), (1..=rounds * per_round).collect::<Vec<_>>());
	});
}

#[test]
fn ceiling_pressure_loses_and_reorders_nothing() {
	// a run that parks repeatedly still owes every acknowledged version exactly once, ascending, with no gap
	let _watchdog = Watchdog::arm("ceiling_pressure_loses_and_reorders_nothing");
	for_each_tier(budget(CUT_RECORDS), budget(CEILING_RECORDS), |case| {
		let total = 400;
		let frozen = case.buffer.flush_guard();
		case.write_all(1..=CEILING_RECORDS + 1);
		let store = case.store.clone();
		let writer = spawn(move || {
			for version in CEILING_RECORDS + 2..=total {
				store.write(&record(version)).unwrap();
			}
		});
		wait_for(&format!("{}: a write over the ceiling never stalled", case.name), || case.stalls() >= 1);
		drop(frozen);
		writer.join(case.name);

		assert!(case.stalls() >= 1, "{}: the run never reached its ceiling", case.name);
		case.flush();
		case.assert_drained();
		assert_eq!(case.versions(), (1..=total).collect::<Vec<_>>());
		case.assert_blocks_partition_versions();
	});
}

#[test]
fn a_parked_write_must_lose_to_a_version_that_landed_while_it_slept() {
	// both writers clear the acceptance check before parking, so a wakeup that skips the re-check admits one
	// version twice
	let _watchdog = Watchdog::arm("a_parked_write_must_lose_to_a_version_that_landed_while_it_slept");
	for_each_tier(budget(CUT_RECORDS), budget(CEILING_RECORDS), |case| {
		let frozen = case.buffer.flush_guard();
		case.write_all(1..=CEILING_RECORDS + 1);
		let contenders: Vec<_> = (0..2)
			.map(|_| {
				let store = case.store.clone();
				spawn(move || store.write(&record(CONTESTED_VERSION)))
			})
			.collect();
		wait_for(&format!("{}: both writes never stalled", case.name), || case.stalls() >= 2);

		drop(frozen);
		let outcomes: Vec<_> = contenders.into_iter().map(|worker| worker.join(case.name)).collect();

		let landed = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
		assert_eq!(
			landed, 1,
			"{}: {landed} of 2 writers landed v{CONTESTED_VERSION}, got {outcomes:?}",
			case.name
		);
		assert!(
			outcomes.iter().any(|outcome| matches!(
				outcome,
				Err(CdcError::DuplicateVersion(CommitVersion(version))) if *version == CONTESTED_VERSION
			)),
			"{}: the loser must be told the version was taken, got {outcomes:?}",
			case.name
		);

		case.flush();
		case.assert_drained();
		case.assert_blocks_partition_versions();
		let mut expected: Vec<u64> = (1..=CEILING_RECORDS + 1).collect();
		expected.push(CONTESTED_VERSION);
		assert_eq!(
			case.versions(),
			expected,
			"{}: the contested version must be readable exactly once",
			case.name
		);
	});
}

#[test]
fn every_writer_at_the_ceiling_completes() {
	// one flush wakes them all at once, so a notify that reached a single waiter leaves the rest parked
	let _watchdog = Watchdog::arm("every_writer_at_the_ceiling_completes");
	let writers = 8;
	let per_writer = 60;
	for_each_tier(budget(CUT_RECORDS), budget(CEILING_RECORDS), |case| {
		let next = Arc::new(AtomicU64::new(1));
		let barrier = Arc::new(Barrier::new(writers));
		let mut workers = Vec::new();
		for _ in 0..writers {
			let store = case.store.clone();
			let next = Arc::clone(&next);
			let barrier = Arc::clone(&barrier);
			workers.push(spawn(move || {
				barrier.wait();
				let mut accepted = Vec::new();
				for _ in 0..per_writer {
					let version = next.fetch_add(1, Ordering::SeqCst);
					if store.write(&record(version)).is_ok() {
						accepted.push(version);
					}
				}
				accepted
			}));
		}

		let mut accepted: Vec<u64> = Vec::new();
		for worker in workers {
			accepted.extend(worker.join(case.name));
		}
		accepted.sort_unstable();

		assert!(
			case.stalls() >= 1,
			"{}: {writers} writers over a {CEILING_RECORDS} record ceiling never stalled",
			case.name
		);
		case.flush();
		case.assert_drained();
		case.assert_blocks_partition_versions();

		let readable = case.versions();
		let lost: Vec<u64> = accepted.iter().copied().filter(|v| !readable.contains(v)).collect();
		let point_readable =
			lost.iter().filter(|v| case.store.read(CommitVersion(**v)).unwrap().is_some()).count();
		assert!(
			lost.is_empty(),
			"{}: {} of {} acknowledged versions are gone from a range read ({point_readable} of them \
			 still answer a point read), first missing {:?}",
			case.name,
			lost.len(),
			accepted.len(),
			&lost[..lost.len().min(8)]
		);
	});
}

#[test]
fn commit_metrics_follow_the_run() {
	// bytes must return to zero, otherwise a residue holds the buffer over its ceiling forever
	let _watchdog = Watchdog::arm("commit_metrics_follow_the_run");
	for_each_tier(budget(CUT_RECORDS), budget(CEILING_RECORDS), |case| {
		assert_eq!(case.metrics(), CdcCommitMetrics::default(), "{}: a fresh buffer", case.name);

		let frozen = case.buffer.flush_guard();
		case.write_all(1..=CEILING_RECORDS + 1);
		let held = case.metrics();
		assert_eq!(held.entries, Count::new(CEILING_RECORDS + 1), "{}: entries held", case.name);
		assert_eq!(held.resident_bytes, budget(CEILING_RECORDS + 1), "{}: bytes held", case.name);
		assert_eq!(held.blocks_cut, 0, "{}: nothing can be cut while the flusher is frozen", case.name);
		assert_eq!(held.stalls, 0, "{}: filling to the ceiling parks nobody", case.name);

		let store = case.store.clone();
		let writer = spawn(move || {
			for version in CEILING_RECORDS + 2..=CEILING_RECORDS + 41 {
				store.write(&record(version)).unwrap();
			}
		});
		wait_for(&format!("{}: a write over the ceiling never stalled", case.name), || case.stalls() >= 1);
		drop(frozen);
		writer.join(case.name);
		case.flush();

		let end = case.metrics();
		assert_eq!(end.entries, Count::ZERO, "{}: entries after the drain", case.name);
		assert_eq!(end.resident_bytes, ByteSize::ZERO, "{}: bytes after the drain", case.name);
		assert!(
			end.blocks_cut >= (CEILING_RECORDS + 41) / CUT_RECORDS,
			"{}: {} blocks for {} records",
			case.name,
			end.blocks_cut,
			CEILING_RECORDS + 41
		);
		assert!(end.stalls >= 1, "{}: the run parked but reported no stall", case.name);
	});
}

#[test]
fn a_buffer_with_no_flusher_never_parks_and_never_stops_growing() {
	// the ceiling only parks while a flusher is attached, so an unattached buffer must grow without bound
	let _watchdog = Watchdog::arm("a_buffer_with_no_flusher_never_parks_and_never_stops_growing");
	let buffer = CdcCommitBufferTier::new(budget(CUT_RECORDS), budget(CEILING_RECORDS));
	let total = 200;
	for version in 1..=total {
		assert!(buffer.append(Arc::new(record(version))), "v{version} was refused");
	}

	assert_eq!(buffer.metrics().stalls, 0, "an unattached buffer cannot be drained, so it must not park");
	assert_eq!(buffer.metrics().entries, Count::new(total));
	assert_eq!(buffer.resident_bytes(), budget(total));
	assert!(
		buffer.resident_bytes() > budget(CEILING_RECORDS * 20),
		"the ceiling held a buffer with no flusher attached"
	);
}
