// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_filter::adaptive::AdaptiveKeyFilter;

fn find_rejected_hash(filter: &AdaptiveKeyFilter, from: u64) -> u64 {
	// A bloom miss is probabilistic, so search a range instead of betting on one arbitrary value;
	// panicking here means the filter answered "maybe present" for 100k unseen keys, which is itself a defect.
	for candidate in from..from + 100_000 {
		if !filter.may_contain(candidate) {
			return candidate;
		}
	}
	panic!("no rejected hash found in range starting at {}", from);
}

#[test]
fn fresh_filter_is_disabled_and_rules_nothing_out() {
	// A fresh filter has no active bloom, and an empty bloom would answer "definitely absent" for every
	// key in existence. Disabled must therefore mean "maybe present" unconditionally, or reads of live
	// rows turn into not-found.
	let filter = AdaptiveKeyFilter::new();

	assert!(!filter.is_enabled());
	for hash in [0u64, 1, 7, u64::MAX, 0x5555_5555_5555_5555, 987_654_321] {
		assert!(filter.may_contain(hash), "disabled filter must not rule out {}", hash);
	}

	let metrics = filter.metrics();
	assert!(!metrics.enabled);
	assert!(!metrics.rebuilding);
	assert_eq!(metrics.rejected, 0);
	assert_eq!(metrics.fill_ratio, 0.0);
	assert_eq!(metrics.estimated_keys, 0);
	assert_eq!(metrics.size_bits, 0);
}

#[test]
fn add_on_disabled_filter_neither_enables_nor_panics() {
	// Writers call add unconditionally, including before the first rebuild has ever run. That must be a
	// no-op: enabling on a write would activate a bloom holding only the keys written since startup and
	// answer "absent" for every pre-existing row.
	let filter = AdaptiveKeyFilter::new();

	for hash in 0..1000u64 {
		filter.add(hash);
	}

	assert!(!filter.is_enabled());
	assert!(filter.may_contain(42));
	assert!(filter.may_contain(u64::MAX - 3));
	assert!(!filter.metrics().enabled);
}

#[test]
fn committed_rebuild_enables_and_rejects_unseen_hashes() {
	// The payoff of the whole design: once enabled the filter must actually answer "definitely absent"
	// for keys that were never fed, otherwise it saves no persistent lookups at all.
	let filter = AdaptiveKeyFilter::new();

	let handle = filter.begin_rebuild(64);
	handle.feed(&[10, 20, 30, 40, 50]);
	filter.commit_rebuild(handle);

	assert!(filter.is_enabled());
	let rejected = find_rejected_hash(&filter, 1_000_000);
	assert!(!filter.may_contain(rejected));

	let metrics = filter.metrics();
	assert!(metrics.enabled);
	assert_eq!(metrics.rebuilds, 1);
	assert!(metrics.fill_ratio > 0.0);
	// size_bits is a bit count, not a word count: reporting words would understate the filter by 64x
	// and make every derived saturation estimate wrong.
	assert!(metrics.size_bits >= 640, "size_bits {} is smaller than the requested 64 keys", metrics.size_bits);
	assert_eq!(metrics.size_bits % 64, 0);
}

#[test]
fn every_fed_hash_survives_the_commit() {
	// feed writes into the handle's own Arc while state.building points at the same filter. If the two
	// ever diverged, commit would swap in a filter missing every fed key: a false negative per live row.
	let filter = AdaptiveKeyFilter::new();

	let hashes: Vec<u64> = (0..500u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).collect();
	let handle = filter.begin_rebuild(500);
	for chunk in hashes.chunks(37) {
		handle.feed(chunk);
	}
	filter.commit_rebuild(handle);

	for hash in &hashes {
		assert!(filter.may_contain(*hash), "fed hash {} answered absent after commit", hash);
	}
}

#[test]
fn writes_during_a_rebuild_land_in_the_new_filter() {
	// The core invariant of this work item. A key written after the scan passed its position exists in
	// neither the scan output nor a non-dual-written building filter, so after the swap it reads as absent
	// while the row is on disk. Dual-write is the only thing preventing that.
	let filter = AdaptiveKeyFilter::new();

	let handle = filter.begin_rebuild(256);
	handle.feed(&[1, 2, 3, 4, 5]);

	let written_during_rebuild = 0xDEAD_BEEF_u64;
	filter.add(written_during_rebuild);

	filter.commit_rebuild(handle);

	assert!(filter.may_contain(written_during_rebuild), "a key written mid-rebuild was lost by the swap");
	for hash in [1u64, 2, 3, 4, 5] {
		assert!(filter.may_contain(hash));
	}
}

#[test]
fn writes_keep_reaching_the_active_filter_during_a_rebuild() {
	// Dual-write means both slots, not "building instead of active". If the in-flight rebuild is aborted,
	// the old active filter is the one that keeps serving, so it must have seen the mid-rebuild writes too.
	let filter = AdaptiveKeyFilter::new();

	let first = filter.begin_rebuild(256);
	first.feed(&[100, 200, 300]);
	filter.commit_rebuild(first);

	let written_during_rebuild = 0xFEED_FACE_u64;
	let second = filter.begin_rebuild(256);
	filter.add(written_during_rebuild);
	filter.abort_rebuild(second);

	assert!(filter.may_contain(written_during_rebuild), "a mid-rebuild write was lost when the rebuild aborted");
}

#[test]
fn in_flight_rebuild_never_answers_queries() {
	// The building filter is incomplete by construction, so consulting it would answer "definitely absent"
	// for keys the scan has not reached yet. Reads must keep coming from active until the swap.
	let filter = AdaptiveKeyFilter::new();

	let first = filter.begin_rebuild(256);
	first.feed(&[7, 8, 9]);
	filter.commit_rebuild(first);

	let known_absent = find_rejected_hash(&filter, 500_000);

	let second = filter.begin_rebuild(256);

	// Present in active, absent from the empty building filter: reading from building would say false.
	assert!(filter.may_contain(7), "query fell through to the incomplete building filter");
	assert!(filter.may_contain(8));
	assert!(filter.may_contain(9));

	// The mirror case: fed only into building, still rejected because active does not know it.
	second.feed(&[known_absent]);
	assert!(!filter.may_contain(known_absent), "query was answered by the building filter instead of active");

	filter.abort_rebuild(second);
}

#[test]
fn abort_leaves_active_untouched() {
	// Abort is the failure path of a scan that never finished. It must drop only the partial filter;
	// disabling or replacing active here would either lose the saved lookups or corrupt reads.
	let filter = AdaptiveKeyFilter::new();

	let first = filter.begin_rebuild(128);
	first.feed(&[11, 22, 33]);
	filter.commit_rebuild(first);

	let known_absent = find_rejected_hash(&filter, 2_000_000);

	let second = filter.begin_rebuild(4096);
	filter.abort_rebuild(second);

	assert!(filter.is_enabled(), "abort disabled a filter that was enabled");
	assert!(filter.may_contain(11));
	assert!(filter.may_contain(22));
	assert!(filter.may_contain(33));
	assert!(!filter.may_contain(known_absent), "abort changed how active answers");

	let metrics = filter.metrics();
	assert_eq!(metrics.aborts, 1);
	assert_eq!(metrics.rebuilds, 1);
	assert!(!metrics.rebuilding);
}

#[test]
fn abort_clears_the_building_slot() {
	// If abort left building populated, the next begin_rebuild would panic and every add would keep
	// writing into a filter nobody will ever swap in.
	let filter = AdaptiveKeyFilter::new();

	let aborted = filter.begin_rebuild(128);
	aborted.feed(&[1, 2, 3]);
	filter.abort_rebuild(aborted);

	assert!(!filter.metrics().rebuilding);
	filter.add(0xABCD);

	let fresh = filter.begin_rebuild(128);
	let written_after_abort = 0x1234_5678_u64;
	filter.add(written_after_abort);
	filter.commit_rebuild(fresh);

	assert!(filter.may_contain(written_after_abort), "add after an abort did not reach the live rebuild");
	assert_eq!(filter.metrics().aborts, 1);
	assert_eq!(filter.metrics().rebuilds, 1);
}

#[test]
#[should_panic(expected = "begin_rebuild called while a rebuild is already in flight")]
fn concurrent_rebuild_panics() {
	// Two rebuilds would leave one handle feeding a filter that state.building no longer points at, so
	// its keys would never be dual-written. That is a caller bug and must fail loud, not silently replace.
	let filter = AdaptiveKeyFilter::new();

	let _first = filter.begin_rebuild(64);
	let _second = filter.begin_rebuild(64);
}

#[test]
fn rebuild_reclaims_space_taken_by_deleted_keys() {
	// The reason rebuilding exists: a bloom cannot delete, so removed keys stay as permanent false
	// positives until the filter is rebuilt from the live key set only.
	let filter = AdaptiveKeyFilter::new();

	let oversubscribed = filter.begin_rebuild(1000);
	for i in 0..20_000u64 {
		oversubscribed.feed(&[i.wrapping_mul(0x517C_C1B7_2722_0A95)]);
	}
	filter.commit_rebuild(oversubscribed);

	let saturated = filter.metrics();
	assert!(saturated.fill_ratio > 0.9, "setup failed to oversubscribe: {}", saturated.fill_ratio);

	let live: Vec<u64> = (0..400u64).map(|i| i.wrapping_mul(0x517C_C1B7_2722_0A95)).collect();
	let compacted = filter.begin_rebuild(500);
	compacted.feed(&live);
	filter.commit_rebuild(compacted);

	let after = filter.metrics();
	assert!(
		after.fill_ratio < saturated.fill_ratio,
		"rebuild did not reclaim stale keys: {} -> {}",
		saturated.fill_ratio,
		after.fill_ratio
	);
	assert!(after.size_bits < saturated.size_bits);
	assert!(after.estimated_keys < saturated.estimated_keys);
	for hash in &live {
		assert!(filter.may_contain(*hash), "live key {} lost by the compacting rebuild", hash);
	}
}

#[test]
fn counters_track_queries_and_savings_only() {
	// rejected is the payoff number: the count of persistent lookups skipped. Counting a maybe-present
	// answer there, or counting writes as queries, makes the metric claim savings that never happened.
	let filter = AdaptiveKeyFilter::new();

	let handle = filter.begin_rebuild(64);
	handle.feed(&[61, 62, 63]);
	filter.commit_rebuild(handle);

	let known_absent = find_rejected_hash(&filter, 3_000_000);

	let before = filter.metrics();
	for _ in 0..5 {
		filter.add(70_000);
		filter.add(80_000);
	}
	let after_writes = filter.metrics();
	assert_eq!(after_writes.queries, before.queries, "add counted as a query");
	assert_eq!(after_writes.rejected, before.rejected, "add counted as a rejection");

	assert!(filter.may_contain(61));
	assert!(filter.may_contain(62));
	let after_hits = filter.metrics();
	assert_eq!(after_hits.queries, before.queries + 2);
	assert_eq!(after_hits.rejected, before.rejected, "a maybe-present answer was counted as rejected");

	assert!(!filter.may_contain(known_absent));
	assert!(!filter.may_contain(known_absent));
	let after_misses = filter.metrics();
	assert_eq!(after_misses.queries, before.queries + 4);
	assert_eq!(after_misses.rejected, before.rejected + 2);
}

#[test]
fn disabled_queries_count_but_never_reject() {
	// A disabled filter saves nothing, so every query must be visible and none of them may claim a
	// skipped lookup.
	let filter = AdaptiveKeyFilter::new();

	for hash in 0..50u64 {
		assert!(filter.may_contain(hash));
	}

	let metrics = filter.metrics();
	assert_eq!(metrics.queries, 50);
	assert_eq!(metrics.rejected, 0);
}

#[test]
fn rebuilding_flag_spans_begin_to_commit() {
	// The maintenance actor reads this flag to decide whether a rebuild is already in flight; a stale
	// true stalls maintenance forever and a stale false invites the double-begin panic.
	let filter = AdaptiveKeyFilter::new();
	assert!(!filter.metrics().rebuilding);

	let handle = filter.begin_rebuild(64);
	assert!(filter.metrics().rebuilding, "rebuilding stayed false after begin_rebuild");

	handle.feed(&[5]);
	assert!(filter.metrics().rebuilding);

	filter.commit_rebuild(handle);
	assert!(!filter.metrics().rebuilding, "rebuilding stayed true after commit_rebuild");

	let aborted = filter.begin_rebuild(64);
	assert!(filter.metrics().rebuilding);
	filter.abort_rebuild(aborted);
	assert!(!filter.metrics().rebuilding, "rebuilding stayed true after abort_rebuild");
}

#[test]
fn concurrent_writers_and_readers_never_lose_a_key() {
	// The hot paths take only a read guard while a rebuild holds a write guard at begin and commit.
	// Dual-write starts at begin_rebuild, so every key written after that point must survive the swap
	// no matter which side of the write guard it landed on.
	use std::{sync::Arc, thread};

	let filter = Arc::new(AdaptiveKeyFilter::new());

	let first = filter.begin_rebuild(4096);
	first.feed(&[0]);
	filter.commit_rebuild(first);

	let handle = filter.begin_rebuild(4096);

	let writers: Vec<_> = (0..4u64)
		.map(|t| {
			let filter = Arc::clone(&filter);
			thread::spawn(move || {
				let mut written = Vec::new();
				for i in 0..250u64 {
					let hash = t * 1_000_000 + i + 1;
					filter.add(hash);
					written.push(hash);
				}
				written
			})
		})
		.collect();

	handle.feed(&[1, 2, 3]);

	let mut all = Vec::new();
	for writer in writers {
		all.extend(writer.join().unwrap());
	}
	filter.commit_rebuild(handle);

	for hash in all {
		assert!(filter.may_contain(hash), "concurrent write {} was lost", hash);
	}
}
