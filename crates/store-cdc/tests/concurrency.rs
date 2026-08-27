// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A record must stay visible across the whole commit-to-persistent handoff, which a single-command testscript cannot
//! exercise.

use std::{
	collections::Bound,
	sync::{
		Arc, Barrier,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread,
};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_store_cdc::{
	error::CdcError,
	storage::{CdcStorage, Cutoff},
	store::CdcStore,
};
use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

mod common;

use common::Fixture;

const BASE_TIMESTAMP: u64 = 1_700_000_000_000_000_000;

const SCAN_BATCH: u64 = 1_000_000;

const SPAN_ROUNDS: u64 = 8;

const SPAN_WRITERS: u64 = 4;

const SPAN_LEN: u64 = 300;

const CLAIM_WRITERS: u64 = 4;

const CLAIM_TOTAL: u64 = 8_000;

const STREAM_TOTAL: u64 = 8_000;

const HEAD_PROBE_LAG: u64 = 3;

const RETENTION_BLOCKS: u64 = 500;

const RETENTION_PER_BLOCK: u64 = 4;

const RETENTION_KEPT_BLOCKS: u64 = 20;

const FLUSH_THREADS: usize = 4;

const FLUSH_TOTAL: u64 = 8_000;

const DUEL_VERSIONS: u64 = 1_000;

const INFLIGHT_APPENDS: u64 = 20_000;

const MIN_ROUNDS: usize = 24;

fn cdc_at(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(BASE_TIMESTAMP + version),
		vec![CdcChange::Insert {
			key: EncodedKey::new(version.to_be_bytes().to_vec()),
			post: EncodedBytes(CowVec::new(version.to_be_bytes().to_vec())),
		}],
	)
}

fn scan(store: &CdcStore) -> Vec<u64> {
	store.read_range(Bound::Unbounded, Bound::Unbounded, SCAN_BATCH)
		.expect("a full scan must not error")
		.items
		.iter()
		.map(|cdc| cdc.version.0)
		.collect()
}

fn first_break(versions: &[u64]) -> Option<String> {
	for pair in versions.windows(2) {
		if pair[1] <= pair[0] {
			return Some(format!("v{} is followed by v{}, which is not ascending", pair[0], pair[1]));
		}
		if pair[1] != pair[0] + 1 {
			return Some(format!(
				"v{} is followed by v{}, so v{} is missing",
				pair[0],
				pair[1],
				pair[0] + 1
			));
		}
	}
	None
}

fn first_mismatch(expected: &[u64], observed: &[u64]) -> Option<String> {
	for (index, (want, got)) in expected.iter().zip(observed.iter()).enumerate() {
		if want != got {
			return Some(format!("at position {index} the log holds v{got} where v{want} was written"));
		}
	}
	if expected.len() != observed.len() {
		return Some(format!("{} records were written but {} came back", expected.len(), observed.len()));
	}
	None
}

mod cases {
	use super::*;

	pub fn writers_racing_the_flusher(fixture: Fixture) {
		// every writer owns a span of its own, so no version is offered twice and a rejection must come from
		// the tier
		let store = fixture.store.clone();
		let mut accepted: Vec<u64> = Vec::new();
		let mut rejected: Vec<u64> = Vec::new();
		let mut unexpected: Vec<u64> = Vec::new();

		for round in 0..SPAN_ROUNDS {
			let base = round * SPAN_WRITERS * SPAN_LEN + 1;
			let stop = Arc::new(AtomicBool::new(false));
			let barrier = Arc::new(Barrier::new(SPAN_WRITERS as usize + 1));

			let flusher = {
				let store = store.clone();
				let stop = Arc::clone(&stop);
				let barrier = Arc::clone(&barrier);
				thread::spawn(move || {
					barrier.wait();
					while !stop.load(Ordering::Acquire) {
						assert!(
							store.flush_pending(),
							"a flush racing the writers must not time out"
						);
					}
				})
			};

			let writers: Vec<_> = (0..SPAN_WRITERS)
				.map(|writer| {
					let store = store.clone();
					let barrier = Arc::clone(&barrier);
					thread::spawn(move || {
						let mut ok = Vec::new();
						let mut duplicate = Vec::new();
						let mut other = Vec::new();
						barrier.wait();
						for offset in 0..SPAN_LEN {
							let version = base + writer * SPAN_LEN + offset;
							match store.write(&cdc_at(version)) {
								Ok(()) => ok.push(version),
								Err(CdcError::DuplicateVersion(reported)) => {
									assert_eq!(
										reported.0, version,
										"a rejection must name the version the caller \
										 offered"
									);
									duplicate.push(version);
								}
								Err(_) => other.push(version),
							}
						}
						(ok, duplicate, other)
					})
				})
				.collect();

			for writer in writers {
				let (ok, duplicate, other) = writer.join().expect("a writing thread must not panic");
				accepted.extend(ok);
				rejected.extend(duplicate);
				unexpected.extend(other);
			}
			stop.store(true, Ordering::Release);
			flusher.join().expect("the flushing thread must not panic");
			assert!(store.flush_pending(), "the round must be drained before the next one starts");
		}

		accepted.sort_unstable();
		let observed = scan(&store);
		let mismatch = first_mismatch(&accepted, &observed);
		assert!(
			mismatch.is_none(),
			"a scan of the whole version space must return every accepted record exactly once and in order, \
			 but {}; a reader that crosses a flush boundary is seeing a gap, a repeat or a record the handoff \
			 dropped",
			mismatch.unwrap_or_default()
		);

		for version in accepted.iter().step_by(97) {
			assert!(
				store.read(CommitVersion(*version)).expect("a point read must not error").is_some(),
				"v{version} was accepted by the commit tier but no tier will answer for it any more"
			);
		}

		assert!(
			unexpected.is_empty(),
			"{} writes failed with something other than a duplicate, starting at v{}",
			unexpected.len(),
			unexpected.first().copied().unwrap_or_default()
		);
		assert!(
			rejected.is_empty(),
			"{} of {} writes were rejected as duplicates although no version was offered twice, starting at \
			 v{}; the flusher seals the commit tier at the highest version it cut, so a concurrent writer whose \
			 version is lower loses its record and is told it was already written",
			rejected.len(),
			rejected.len() + accepted.len(),
			rejected.first().copied().unwrap_or_default()
		);
	}

	pub fn versions_claimed_in_order_survive_the_flusher(fixture: Fixture) {
		// a version is claimed and written straight away, so only a flush cutting between two claims can refuse
		// one
		let store = fixture.store.clone();
		let next = Arc::new(AtomicU64::new(1));
		let stop = Arc::new(AtomicBool::new(false));
		let barrier = Arc::new(Barrier::new(CLAIM_WRITERS as usize + 1));

		let flusher = {
			let store = store.clone();
			let stop = Arc::clone(&stop);
			let barrier = Arc::clone(&barrier);
			thread::spawn(move || {
				barrier.wait();
				while !stop.load(Ordering::Acquire) {
					assert!(store.flush_pending(), "a flush racing the writers must not time out");
				}
			})
		};

		let writers: Vec<_> = (0..CLAIM_WRITERS)
			.map(|_| {
				let store = store.clone();
				let next = Arc::clone(&next);
				let barrier = Arc::clone(&barrier);
				thread::spawn(move || {
					let mut ok = Vec::new();
					let mut rejected = Vec::new();
					barrier.wait();
					loop {
						let version = next.fetch_add(1, Ordering::SeqCst);
						if version > CLAIM_TOTAL {
							break;
						}
						match store.write(&cdc_at(version)) {
							Ok(()) => ok.push(version),
							Err(_) => rejected.push(version),
						}
					}
					(ok, rejected)
				})
			})
			.collect();

		let mut accepted: Vec<u64> = Vec::new();
		let mut rejected: Vec<u64> = Vec::new();
		for writer in writers {
			let (ok, refused) = writer.join().expect("a writing thread must not panic");
			accepted.extend(ok);
			rejected.extend(refused);
		}
		stop.store(true, Ordering::Release);
		flusher.join().expect("the flushing thread must not panic");
		assert!(store.flush_pending(), "the last claims must reach the flusher");

		accepted.sort_unstable();
		let observed = scan(&store);
		let mismatch = first_mismatch(&accepted, &observed);
		assert!(
			mismatch.is_none(),
			"a scan must return every accepted record exactly once and in order, but {}",
			mismatch.unwrap_or_default()
		);
		rejected.sort_unstable();
		assert!(
			rejected.is_empty(),
			"{} of {CLAIM_TOTAL} writes were refused, starting at v{}; the version was claimed before any \
			 higher one was written, so the flusher sealed over a record the commit path had already handed out \
			 and that change is now missing from the log",
			rejected.len(),
			rejected.first().copied().unwrap_or_default()
		);
	}

	pub fn a_scan_never_loses_a_version_to_a_flush(fixture: Fixture) {
		// nothing is ever dropped here, so a scan that shrinks means the merge walk handed off at a stale floor
		let store = fixture.store.clone();
		let done = Arc::new(AtomicBool::new(false));
		let barrier = Arc::new(Barrier::new(3));

		let flusher = {
			let store = store.clone();
			let done = Arc::clone(&done);
			let barrier = Arc::clone(&barrier);
			thread::spawn(move || {
				barrier.wait();
				while !done.load(Ordering::Acquire) {
					assert!(store.flush_pending(), "a flush racing the reader must not time out");
				}
			})
		};

		let reader = {
			let store = store.clone();
			let done = Arc::clone(&done);
			let barrier = Arc::clone(&barrier);
			thread::spawn(move || {
				let mut scans = 0usize;
				let mut widest = 0usize;
				let mut violation: Option<String> = None;
				barrier.wait();
				loop {
					let finished = done.load(Ordering::Acquire);
					let versions = scan(&store);
					scans += 1;
					if violation.is_none() {
						if let Some(gap) = first_break(&versions) {
							violation =
								Some(format!("a scan came back with a hole: {gap}"));
						} else if versions.first().is_some_and(|first| *first != 1) {
							violation = Some(format!(
								"a scan started at v{} although nothing was ever dropped",
								versions[0]
							));
						} else if versions.len() < widest {
							violation = Some(format!(
								"a scan returned {} records after an earlier scan had \
								 already returned {widest}",
								versions.len()
							));
						}
					}
					widest = widest.max(versions.len());
					if finished && scans >= MIN_ROUNDS {
						break;
					}
				}
				(scans, violation)
			})
		};

		barrier.wait();
		for version in 1..=STREAM_TOTAL {
			store.write(&cdc_at(version)).expect("a single writer advances the version space in order");
		}
		done.store(true, Ordering::Release);
		flusher.join().expect("the flushing thread must not panic");
		let (scans, violation) = reader.join().expect("the reading thread must not panic");

		assert!(
			scans >= MIN_ROUNDS,
			"the reader must have scanned repeatedly, otherwise this test asserts nothing"
		);
		assert!(
			violation.is_none(),
			"a reader that crosses a flush boundary must never lose a record it could already see: {}",
			violation.unwrap_or_default()
		);
		assert!(store.flush_pending(), "the tail of the stream must reach the flusher");
		let observed = scan(&store);
		let expected: Vec<u64> = (1..=STREAM_TOTAL).collect();
		let mismatch = first_mismatch(&expected, &observed);
		assert!(
			mismatch.is_none(),
			"once the race is over the whole stream must be readable, but {}",
			mismatch.unwrap_or_default()
		);
	}

	pub fn a_read_of_a_flushing_version_never_returns_none(fixture: Fixture) {
		// take_for_flush empties the live map before the block lands, so a read in that window must find the
		// batch
		let store = fixture.store.clone();
		let done = Arc::new(AtomicBool::new(false));
		let barrier = Arc::new(Barrier::new(3));

		let flusher = {
			let store = store.clone();
			let done = Arc::clone(&done);
			let barrier = Arc::clone(&barrier);
			thread::spawn(move || {
				barrier.wait();
				while !done.load(Ordering::Acquire) {
					assert!(store.flush_pending(), "a flush racing the reader must not time out");
				}
			})
		};

		let reader = {
			let store = store.clone();
			let done = Arc::clone(&done);
			let barrier = Arc::clone(&barrier);
			thread::spawn(move || {
				let mut reads = 0usize;
				let mut missing_head = 0usize;
				let mut missing_probe = 0usize;
				let mut wrong_record = 0usize;
				barrier.wait();
				while !done.load(Ordering::Acquire) || reads < MIN_ROUNDS {
					let Some(head) = store.max_version().expect("a bound read must not error")
					else {
						continue;
					};
					reads += 1;
					match store.read(head).expect("a point read must not error") {
						Some(cdc) if cdc.version == head => {}
						Some(_) => wrong_record += 1,
						None => missing_head += 1,
					}
					let probe = CommitVersion(head.0.saturating_sub(HEAD_PROBE_LAG));
					if probe.0 == 0 {
						continue;
					}
					reads += 1;
					match store.read(probe).expect("a point read must not error") {
						Some(cdc) if cdc.version == probe => {}
						Some(_) => wrong_record += 1,
						None => missing_probe += 1,
					}
				}
				(reads, missing_head, missing_probe, wrong_record)
			})
		};

		barrier.wait();
		for version in 1..=STREAM_TOTAL {
			store.write(&cdc_at(version)).expect("a single writer advances the version space in order");
		}
		done.store(true, Ordering::Release);
		flusher.join().expect("the flushing thread must not panic");
		let (reads, missing_head, missing_probe, wrong_record) =
			reader.join().expect("the reader must not panic");

		assert!(
			reads >= MIN_ROUNDS,
			"the reader must have read repeatedly, otherwise this test asserts nothing"
		);
		assert_eq!(
			missing_head, 0,
			"the highest version the store reports must always be readable; a read that misses it fell between \
			 the live map and the persistent tier while the flush held the records"
		);
		assert_eq!(
			missing_probe, 0,
			"a record just behind the head sits exactly in the batch being flushed, and it must stay readable \
			 for the whole handoff rather than vanish for the width of one block write"
		);
		assert_eq!(wrong_record, 0, "a read must answer with the version it was asked for, never a neighbour");
	}

	pub fn retention_never_serves_a_record_it_dropped(fixture: Fixture) {
		// retention forgets the read buffer and then deletes the blocks, so a reader must never reinstate one
		let store = fixture.store.clone();
		for block in 0..RETENTION_BLOCKS {
			for offset in 1..=RETENTION_PER_BLOCK {
				let version = block * RETENTION_PER_BLOCK + offset;
				store.write(&cdc_at(version)).expect("the seed must be written in order");
			}
			assert!(store.flush_pending(), "each seed group must become a block of its own");
		}

		let done = Arc::new(AtomicBool::new(false));
		let barrier = Arc::new(Barrier::new(3));

		let scanner = {
			let store = store.clone();
			let done = Arc::clone(&done);
			let barrier = Arc::clone(&barrier);
			thread::spawn(move || {
				let mut scans = 0usize;
				let mut violation: Option<String> = None;
				barrier.wait();
				while !done.load(Ordering::Acquire) || scans < MIN_ROUNDS {
					let floor = store.truncated_before().expect("a floor read must not error");
					let versions = scan(&store);
					scans += 1;
					if violation.is_some() {
						continue;
					}
					if let Some(gap) = first_break(&versions) {
						violation = Some(format!("a scan came back torn: {gap}"));
					} else if versions.first().is_some_and(|first| *first < floor.0) {
						violation = Some(format!(
							"a scan returned v{} although retention had already reported \
							 everything below v{} as gone",
							versions[0], floor.0
						));
					}
				}
				(scans, violation)
			})
		};

		let prober = {
			let store = store.clone();
			let done = Arc::clone(&done);
			let barrier = Arc::clone(&barrier);
			thread::spawn(move || {
				let mut probes = 0usize;
				let mut violation: Option<String> = None;
				barrier.wait();
				while !done.load(Ordering::Acquire) || probes < MIN_ROUNDS {
					let floor = store.truncated_before().expect("a floor read must not error");
					if floor.0 == 0 {
						continue;
					}
					let dropped = CommitVersion(floor.0 - 1);
					probes += 1;
					if violation.is_none()
						&& store.read(dropped).expect("a point read must not error").is_some()
					{
						violation = Some(format!(
							"v{} answered from a block retention had already deleted",
							dropped.0
						));
					}
				}
				(probes, violation)
			})
		};

		barrier.wait();
		let mut dropper_violation: Option<String> = None;
		for block in 0..(RETENTION_BLOCKS - RETENTION_KEPT_BLOCKS) {
			let cutoff = CommitVersion((block + 1) * RETENTION_PER_BLOCK + 1);
			store.drop_before(Cutoff::Version(cutoff), 2).expect("retention must not error");
			let floor = store.truncated_before().expect("a floor read must not error");
			if floor.0 == 0 || dropper_violation.is_some() {
				continue;
			}
			for version in [1, floor.0 / 2, floor.0 - 1] {
				if version == 0 || version >= floor.0 {
					continue;
				}
				if store.read(CommitVersion(version)).expect("a point read must not error").is_some() {
					dropper_violation = Some(format!(
						"v{version} was still readable after retention reported everything below \
						 v{} as dropped",
						floor.0
					));
					break;
				}
			}
		}
		done.store(true, Ordering::Release);
		let (scans, scan_violation) = scanner.join().expect("the scanning thread must not panic");
		let (probes, probe_violation) = prober.join().expect("the probing thread must not panic");

		assert!(
			scans >= MIN_ROUNDS,
			"the scanner must have run repeatedly, otherwise this test asserts nothing"
		);
		assert!(
			probes >= MIN_ROUNDS,
			"the prober must have run repeatedly, otherwise this test asserts nothing"
		);
		assert!(
			scan_violation.is_none(),
			"a reader racing retention must never see a torn range or a record below the reported floor: {}",
			scan_violation.unwrap_or_default()
		);
		assert!(
			probe_violation.is_none(),
			"a record whose block retention has deleted must never come back: {}; the read buffer is warmed \
			 after the block is inflated, so a reader can reinstate a block that was invalidated in between",
			probe_violation.unwrap_or_default()
		);
		assert!(
			dropper_violation.is_none(),
			"retention must not report a block as dropped while a reader can still be served from it: {}",
			dropper_violation.unwrap_or_default()
		);
	}

	pub fn concurrent_flushes_neither_deadlock_nor_double_cut(fixture: Fixture) {
		// a run cut twice puts one version in two blocks, and a consumer replays a change it already applied
		let store = fixture.store.clone();
		let done = Arc::new(AtomicBool::new(false));
		let barrier = Arc::new(Barrier::new(FLUSH_THREADS + 1));

		let flushers: Vec<_> = (0..FLUSH_THREADS)
			.map(|_| {
				let store = store.clone();
				let done = Arc::clone(&done);
				let barrier = Arc::clone(&barrier);
				thread::spawn(move || {
					let mut calls = 0usize;
					let mut timeouts = 0usize;
					barrier.wait();
					while !done.load(Ordering::Acquire) || calls < MIN_ROUNDS {
						calls += 1;
						if !store.flush_pending() {
							timeouts += 1;
						}
					}
					(calls, timeouts)
				})
			})
			.collect();

		barrier.wait();
		for version in 1..=FLUSH_TOTAL {
			store.write(&cdc_at(version)).expect("a single writer advances the version space in order");
		}
		done.store(true, Ordering::Release);
		let mut calls = 0usize;
		let mut timeouts = 0usize;
		for flusher in flushers {
			let (made, timed_out) = flusher.join().expect("a flushing thread must not panic");
			calls += made;
			timeouts += timed_out;
		}
		assert!(store.flush_pending(), "the tail must reach the flusher once the race is over");

		assert!(
			calls >= FLUSH_THREADS * MIN_ROUNDS,
			"the flushers must have run, otherwise this test asserts nothing"
		);
		assert_eq!(
			timeouts, 0,
			"a flush that never returns means two flushers took the same guard in the opposite order and the \
			 log stops being drained"
		);

		let summaries = fixture
			.persistent
			.summaries_from(CommitVersion(0), 1_000_000)
			.expect("summaries must not error");
		let cut = store.commit_metrics().blocks_cut;
		assert_eq!(
			summaries.len() as u64,
			cut,
			"the commit tier counted {cut} blocks but the persistent tier holds {}; a run cut twice is written \
			 twice or lost between the two counts",
			summaries.len()
		);
		let mut previous: Option<u64> = None;
		let mut total = 0u64;
		for summary in &summaries {
			assert!(
				summary.min_version <= summary.max_version,
				"block [{}..{}] is inverted",
				summary.min_version.0,
				summary.max_version.0
			);
			if let Some(high) = previous {
				assert!(
					summary.min_version.0 > high,
					"block [{}..{}] overlaps the block ending at v{high}, so a version sits in two \
					 blocks at once",
					summary.min_version.0,
					summary.max_version.0
				);
			}
			previous = Some(summary.max_version.0);
			total += summary.count.as_u64();
		}
		assert_eq!(
			total, FLUSH_TOTAL,
			"the blocks carry {total} records for {FLUSH_TOTAL} writes, so a run was cut twice or dropped"
		);

		let expected: Vec<u64> = (1..=FLUSH_TOTAL).collect();
		let observed = scan(&store);
		let mismatch = first_mismatch(&expected, &observed);
		assert!(
			mismatch.is_none(),
			"a scan after concurrent flushes must return the whole space exactly once, but {}",
			mismatch.unwrap_or_default()
		);
	}

	pub fn two_writers_of_one_version_produce_one_winner(fixture: Fixture) {
		// the log carries exactly one record per version, so a contested version must be taken once and refused
		// once
		let store = fixture.store.clone();
		let done = Arc::new(AtomicBool::new(false));
		let barrier = Arc::new(Barrier::new(2));

		let flusher = {
			let store = store.clone();
			let done = Arc::clone(&done);
			thread::spawn(move || {
				while !done.load(Ordering::Acquire) {
					assert!(store.flush_pending(), "a flush racing the duel must not time out");
				}
			})
		};

		let duellists: Vec<_> = (0..2)
			.map(|_| {
				let store = store.clone();
				let barrier = Arc::clone(&barrier);
				thread::spawn(move || {
					let mut outcomes = Vec::with_capacity(DUEL_VERSIONS as usize);
					for version in 1..=DUEL_VERSIONS {
						barrier.wait();
						outcomes.push(match store.write(&cdc_at(version)) {
							Ok(()) => 0u8,
							Err(CdcError::DuplicateVersion(reported))
								if reported.0 == version =>
							{
								1
							}
							Err(_) => 2,
						});
					}
					outcomes
				})
			})
			.collect();

		let mut results = Vec::new();
		for duellist in duellists {
			results.push(duellist.join().expect("a duelling thread must not panic"));
		}
		done.store(true, Ordering::Release);
		flusher.join().expect("the flushing thread must not panic");
		assert!(store.flush_pending(), "the duel must be drained before the log is checked");

		for index in 0..DUEL_VERSIONS as usize {
			let left = results[0][index];
			let right = results[1][index];
			let version = index as u64 + 1;
			assert!(
				left != 2 && right != 2,
				"v{version} was refused with something other than a duplicate, so the loser cannot tell \
				 that the version was taken"
			);
			assert!(
				left == 0 || right == 0,
				"v{version} was refused by both writers, so the change never reached the log although \
				 nobody had written that version"
			);
			assert!(
				left != 0 || right != 0,
				"v{version} was accepted by both writers, so one record silently replaced the other and \
				 a consumer sees only one of the two changes"
			);
		}

		let expected: Vec<u64> = (1..=DUEL_VERSIONS).collect();
		let observed = scan(&store);
		let mismatch = first_mismatch(&expected, &observed);
		assert!(
			mismatch.is_none(),
			"every duelled version must appear once and only once, but {}",
			mismatch.unwrap_or_default()
		);
	}

	pub fn appends_complete_while_a_flush_is_in_flight(fixture: Fixture) {
		// staged runs after the flush took the live map and before it sealed, so the append is never in flight by timing
		let store = fixture.store.clone();
		store.write(&cdc_at(1)).expect("the first record must reach the live map before any flush runs");

		let mut next = 2u64;
		let mut refused: Vec<String> = Vec::new();
		store.flush_staged(&mut || {
			if next > INFLIGHT_APPENDS {
				return;
			}
			if let Err(err) = store.write(&cdc_at(next)) {
				refused.push(format!("v{next} was refused with {err:?}"));
			}
			next += 1;
		});

		assert!(
			refused.is_empty(),
			"a record appended while the flush held a batch in flight was refused, so a writer cannot make \
			 progress through a flush: {refused:?}"
		);
		assert_eq!(
			next,
			INFLIGHT_APPENDS + 1,
			"the flush stopped after v{}, so it never came back for the records appended while it was in \
			 flight; writes are being serialised behind the flush instead of running through it",
			next - 1
		);

		let metrics = store.commit_metrics();
		assert_eq!(
			metrics.stalls, 0,
			"no append may have waited on the flusher; the commit tier only stalls a writer above its ceiling \
			 and this test never reaches it"
		);
		assert_eq!(
			metrics.blocks_cut, INFLIGHT_APPENDS,
			"each staged append lands after its own flush already took the live map, so the same pass must \
			 seal one block per append instead of folding them into the batch it had cut"
		);

		let expected: Vec<u64> = (1..=INFLIGHT_APPENDS).collect();
		let observed = scan(&store);
		let mismatch = first_mismatch(&expected, &observed);
		assert!(
			mismatch.is_none(),
			"every record appended during the flush must appear once and only once, but {}",
			mismatch.unwrap_or_default()
		);
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
		writers_racing_the_flusher,
		versions_claimed_in_order_survive_the_flusher,
		a_scan_never_loses_a_version_to_a_flush,
		a_read_of_a_flushing_version_never_returns_none,
		retention_never_serves_a_record_it_dropped,
		concurrent_flushes_neither_deadlock_nor_double_cut,
		two_writers_of_one_version_produce_one_winner,
		appends_complete_while_a_flush_is_in_flight,
	]
);
