// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The key filter in front of the multi store's persistent tier. A false positive costs one wasted sqlite
//! read; a false negative makes the store answer "no such row" for a key that is on disk, which is silent
//! data loss. Every test here aims at the false-negative direction. Rebuilds are driven by stepping a
//! RebuildDriver rather than by waiting on the maintenance actor, because the driver is synchronous and the
//! actor is not.

use std::collections::HashMap;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{EntryKind, MultiVersionGet, classify_key},
	},
	util::bloom::hash_item,
};
use reifydb_filter::{
	config::FilterConfig,
	driver::{DriverProgress, RebuildDriver},
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, shutdown::Shutdown};
use reifydb_sqlite::SqliteConfig;
use reifydb_store::filter::KeyFilter;
use reifydb_store_commit::{TierBatch, store::CommitStore};
use reifydb_store_multi::{
	config::{CommitStoreConfig, MultiStoreConfig, PersistentConfig},
	filter::{ARMED_CAPACITY_KEYS, MultiKeys, source::MultiCurrentKeySource},
	store::StandardMultiStore,
	tier::{
		TierStorage, persistent::sqlite::storage::SqlitePersistentStorage, point::MultiPointConfig,
		range::MultiRangeConfig,
	},
};
use reifydb_value::util::cowvec::CowVec;

const OTHER: EntryKind = EntryKind::Source(StorageId::Table(TableId(1)));

fn key(n: u64) -> EncodedKey {
	EncodedKey::new(n.to_be_bytes())
}

fn body(n: u64) -> CowVec<u8> {
	CowVec::new(format!("v{n}").into_bytes())
}

fn batch(table: EntryKind, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> TierBatch {
	let mut out: TierBatch = HashMap::new();
	out.insert(table, entries);
	out
}

fn store_at(config: SqliteConfig) -> StandardMultiStore {
	// The actor system is leaked so the registered filter actor outlives the helper; every row a test
	// wants in sqlite is written straight through the persistent tier, never through the commit buffer.
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let event_bus = EventBus::new(&spawner);
	std::mem::forget(actor_system);
	StandardMultiStore::new(MultiStoreConfig {
		commit: CommitStoreConfig {
			storage: CommitStore::new(),
		},
		persistent: Some(PersistentConfig::sqlite(config)),
		point: Some(MultiPointConfig::testing()),
		range: Some(MultiRangeConfig::testing()),
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus,
		spawner,
		clock,
	})
	.expect("the multi store could not be opened")
}

fn rebuilding_config(scan_budget: usize) -> FilterConfig {
	// fill_trigger 0.0 makes every evaluate() start a rebuild, so a test can force a second pass over an
	// already enabled filter; the tight sizing keeps the false-positive rate low enough that a key the
	// scan dropped answers absent rather than being masked by a collision.
	FilterConfig {
		scan_budget,
		size_headroom: 2.0,
		min_size_keys: 1,
		fill_trigger: 0.0,
		drift_trigger: 1.0,
		..FilterConfig::default()
	}
}

fn rebuild(storage: &SqlitePersistentStorage, config: FilterConfig) {
	// Drives one full cycle to the swap. The cap turns a driver that decides Idle, or never exhausts the
	// source, into a failure rather than a hang.
	let mut driver = RebuildDriver::new(
		storage.filter().handle(),
		Box::new(MultiCurrentKeySource::new(storage.clone())),
		config,
	);
	for _ in 0..10_000 {
		if driver.step() == DriverProgress::Committed {
			return;
		}
	}
	panic!("multi current filter rebuild never committed");
}

#[test]
fn a_key_written_through_the_flush_path_is_afterwards_maybe_present() {
	// set_collecting_accepted is the commit-time flush into the persistent tier. Arming leans entirely on
	// the write paths feeding the filter, so a flush that skipped add() would leave the row on disk and
	// unreachable through the filter for as long as it takes the next rebuild to land.
	let (storage, _guard) = SqlitePersistentStorage::in_memory();

	assert!(storage.filter().metrics().enabled, "an empty database at open must arm the filter");
	assert!(
		!storage.filter().may_contain((EntryKind::Multi, &key(7))),
		"an armed empty filter rules out every key"
	);

	storage.set_collecting_accepted(CommitVersion(5), batch(EntryKind::Multi, vec![(key(7), Some(body(7)))]))
		.expect("the flush write failed");

	assert!(
		storage.filter().may_contain((EntryKind::Multi, &key(7))),
		"the commit flush path did not feed the armed filter"
	);
	assert!(
		!storage.filter().may_contain((EntryKind::Multi, &key(8))),
		"a neighbouring key nothing wrote answered present"
	);
}

#[test]
fn a_key_written_through_the_sweep_path_is_afterwards_maybe_present() {
	// persist_sweep has its own call into the shared upsert loop and carries a version per batch, so it
	// must feed the filter independently of the commit flush path or every swept key becomes a false
	// negative until the next rebuild.
	let (storage, _guard) = SqlitePersistentStorage::in_memory();

	storage.persist_sweep(vec![(
		CommitVersion(9),
		batch(EntryKind::Multi, vec![(key(11), Some(body(11))), (key(12), Some(body(12)))]),
	)])
	.expect("the sweep write failed");

	assert!(storage.filter().may_contain((EntryKind::Multi, &key(11))), "the sweep path did not feed the filter");
	assert!(storage.filter().may_contain((EntryKind::Multi, &key(12))), "the sweep path did not feed the filter");
	assert!(
		!storage.filter().may_contain((EntryKind::Multi, &key(13))),
		"a neighbouring key nothing wrote answered present"
	);
}

#[test]
fn every_key_of_a_batch_large_enough_to_take_the_chunked_upsert_is_fed_to_the_filter() {
	// The upsert loop splits at UPSERT_CHUNK (100): full chunks go through the multi-row statement and the
	// leftovers through the single-row one. They are two separate loops, so a batch small enough to stay in
	// the remainder proves nothing about the chunked branch, and a key written only by the chunked branch
	// would be a permanent false negative.
	let (storage, _guard) = SqlitePersistentStorage::in_memory();
	let entries: Vec<(EncodedKey, Option<CowVec<u8>>)> = (0..250u64).map(|n| (key(n), Some(body(n)))).collect();

	storage.set_collecting_accepted(CommitVersion(1), batch(EntryKind::Multi, entries))
		.expect("the chunked flush write failed");

	for n in 0..250u64 {
		assert!(
			storage.filter().may_contain((EntryKind::Multi, &key(n))),
			"key {n} was written but the chunked upsert loop never fed it to the filter"
		);
	}
	assert!(
		!storage.filter().may_contain((EntryKind::Multi, &key(999))),
		"a key nothing wrote answered present, so the filter is saturated and proves nothing"
	);
}

#[test]
fn a_key_written_through_the_tier_set_path_is_afterwards_maybe_present() {
	// TierStorage::set is the trait-level entry point the store layer reaches the persistent tier through;
	// it delegates to set_collecting_accepted, and a delegation that grew its own upsert loop would drop
	// the filter feed silently.
	let (storage, _guard) = SqlitePersistentStorage::in_memory();

	TierStorage::set(&storage, CommitVersion(3), batch(OTHER, vec![(key(21), Some(body(21)))]))
		.expect("the tier set write failed");

	assert!(storage.filter().may_contain((OTHER, &key(21))), "the tier set path did not feed the filter");
}

#[test]
fn a_store_opened_on_empty_tables_starts_armed_and_rules_out_a_key_nothing_ever_wrote() {
	// An empty database is the one case where an active-but-empty bloom is telling the truth: it answers
	// "definitely absent" for every key, and every key really is absent. The rejected counter is the proof
	// that the read was short circuited rather than served by sqlite returning no row.
	let (config, _guard) = SqliteConfig::in_memory();
	let store = store_at(config);
	let persistent = store.persistent().expect("the test store is configured with a persistent tier");

	assert!(persistent.filter().metrics().enabled, "an empty database at open must arm the filter");

	let before = persistent.filter().metrics();
	assert!(
		MultiVersionGet::get(&store, &key(404), CommitVersion(u64::MAX)).expect("the read failed").is_none(),
		"a key nothing ever wrote must read as absent"
	);
	let after = persistent.filter().metrics();
	assert_eq!(
		after.rejected,
		before.rejected + 1,
		"the read reached sqlite instead of being ruled out by the armed filter"
	);
}

#[test]
fn a_store_opened_on_populated_tables_starts_disabled_and_serves_back_every_existing_row() {
	// The corruption case. A freshly built bloom holds none of the keys already on disk, so arming one over
	// a populated table makes may_contain answer false for every durable row and the store reports the
	// entries as missing until the first background rebuild lands. Rows that exist reading back as absent
	// is silent data loss, which is why the gate is an existence probe and not "always arm".
	let (config, _guard) = SqliteConfig::in_memory();
	let table = classify_key(&key(0));
	{
		let storage = SqlitePersistentStorage::new(config.clone());
		let entries = (0..32u64).map(|n| (key(n), Some(body(n)))).collect();
		storage.set_collecting_accepted(CommitVersion(1), batch(table, entries)).expect("setup write failed");
		storage.shutdown();
	}

	let store = store_at(config);
	let persistent = store.persistent().expect("the test store is configured with a persistent tier");

	assert!(
		!persistent.filter().metrics().enabled,
		"a database that already holds rows must open with a disabled, permissive filter"
	);

	for n in 0..32u64 {
		let found = MultiVersionGet::get(&store, &key(n), CommitVersion(u64::MAX))
			.expect("the read failed")
			.unwrap_or_else(|| {
				panic!("row {n} exists in sqlite but the reopened store reported it absent")
			});
		assert_eq!(found.bytes.as_slice(), body(n).as_slice());
	}

	assert_eq!(
		persistent.filter().metrics().rejected,
		0,
		"a disabled filter ruled a key out, so it is no longer permissive"
	);
}

#[test]
fn a_removed_key_is_reported_absent_after_a_rebuild() {
	// The persistent tier is terminal, so ruling a removed key out is never a fallback to a stale value.
	let (storage, _guard) = SqlitePersistentStorage::in_memory();
	storage.set_collecting_accepted(
		CommitVersion(4),
		batch(EntryKind::Multi, vec![(key(1), None), (key(2), Some(body(2)))]),
	)
	.expect("setup write failed");

	rebuild(&storage, rebuilding_config(8));

	assert!(storage.filter().metrics().enabled, "a committed rebuild must leave the filter active");
	assert!(storage.filter().may_contain((EntryKind::Multi, &key(2))), "the rebuild dropped a live key");
	assert!(
		matches!(
			storage.get(EntryKind::Multi, key(1).as_slice(), CommitVersion(u64::MAX)),
			Ok(reifydb_store_commit::VersionedGetResult::NotFound)
		),
		"the removed key must read as absent through the tier after the swap"
	);
}

#[test]
fn the_rebuild_source_hashes_a_key_exactly_as_the_read_path_does() {
	// The one invariant the whole port rests on: the rebuild source, the write path and the read path must
	// hash the same (entry kind, key) pair identically. If they diverge, the rebuilt filter answers absent
	// for keys that exist and the store silently loses rows. The literal comparison catches a change to the
	// hashed expression; the may_contain round trip catches the two sides drifting apart at all.
	use reifydb_filter::source::KeyFilterSource;

	let (storage, _guard) = SqlitePersistentStorage::in_memory();
	storage.set_collecting_accepted(CommitVersion(2), batch(EntryKind::Multi, vec![(key(3), Some(body(3)))]))
		.expect("setup write failed");

	let mut source = MultiCurrentKeySource::new(storage.clone());
	let slice = source.next_slice(64);

	assert_eq!(slice.hashes.len(), 1, "the source must yield the one key that was written");
	assert!(slice.exhausted, "a slice short of its budget on the last table must report the scan finished");
	assert_eq!(
		slice.hashes[0],
		hash_item(&(EntryKind::Multi, key(3).as_slice())),
		"the source no longer hashes the pair the read path hashes"
	);

	rebuild(&storage, rebuilding_config(64));
	assert!(
		storage.filter().may_contain((EntryKind::Multi, &key(3))),
		"a filter built purely from the source's hashes answered absent for a key the read path would test"
	);
}

#[test]
fn identical_key_bytes_in_two_entry_kinds_stay_separately_reachable_across_a_rebuild() {
	// Every entry kind owns its own sqlite table, so the same key bytes name two unrelated rows. Hashing
	// the key alone would collapse them into one filter entry, and a rebuild that walked only one table
	// would then answer present for a key it never scanned; hashing the version in instead would mint a new
	// entry per write and rule the older one out. Both rows must survive the swap.
	let (storage, _guard) = SqlitePersistentStorage::in_memory();
	for n in 0..24u64 {
		storage.set_collecting_accepted(
			CommitVersion(n + 1),
			batch(EntryKind::Multi, vec![(key(n), Some(body(n)))]),
		)
		.expect("setup write failed");
		storage.set_collecting_accepted(CommitVersion(n + 1), batch(OTHER, vec![(key(n), Some(body(n)))]))
			.expect("setup write failed");
	}

	assert_ne!(
		hash_item(&(EntryKind::Multi, key(0).as_slice())),
		hash_item(&(OTHER, key(0).as_slice())),
		"two entry kinds sharing key bytes must not hash to the same filter entry"
	);

	rebuild(&storage, rebuilding_config(5));

	for n in 0..24u64 {
		assert!(
			storage.filter().may_contain((EntryKind::Multi, &key(n))),
			"multi key {n} was lost by the rebuild"
		);
		assert!(storage.filter().may_contain((OTHER, &key(n))), "source key {n} was lost by the rebuild");
		assert!(
			storage.get(EntryKind::Multi, key(n).as_slice(), CommitVersion(u64::MAX))
				.expect("the read failed")
				.value()
				.is_some(),
			"multi row {n} became unreadable"
		);
		assert!(
			storage.get(OTHER, key(n).as_slice(), CommitVersion(u64::MAX))
				.expect("the read failed")
				.value()
				.is_some(),
			"source row {n} became unreadable"
		);
	}
}

#[test]
fn an_armed_filter_is_left_alone_until_it_fills_and_is_then_resized_from_the_live_key_count() {
	// Two claims about the unchanged driver against this store's source. First, an armed filter is already
	// enabled, so evaluate() skips the initial-build branch and stays Idle instead of rebuilding a filter
	// that is answering correctly. Second, once load pushes it past fill_trigger the driver rebuilds and
	// sizes the new bloom from the source's live key count, so size_bits must FALL: the armed allocation is
	// a fixed capacity guess made before any row existed. A capacity smaller than the real
	// ARMED_CAPACITY_KEYS is used here only so the trigger can be reached without writing 700k rows.
	let (storage, _guard) = SqlitePersistentStorage::in_memory();
	for n in 0..64u64 {
		storage.set_collecting_accepted(
			CommitVersion(n + 1),
			batch(EntryKind::Multi, vec![(key(n), Some(body(n)))]),
		)
		.expect("setup write failed");
	}

	let filter = KeyFilter::<MultiKeys>::armed(4096);
	let armed_bits = filter.metrics().size_bits;
	let mut driver = RebuildDriver::new(
		filter.handle(),
		Box::new(MultiCurrentKeySource::new(storage.clone())),
		FilterConfig::default(),
	);

	assert_eq!(
		driver.step(),
		DriverProgress::Idle,
		"an armed filter is enabled, so the driver must not treat it as needing an initial build"
	);

	for i in 0..4000u64 {
		filter.handle().add(i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
	}
	let filled = filter.metrics().fill_ratio;
	assert!(filled > FilterConfig::default().fill_trigger, "setup left the filter below the trigger: {filled}");

	let mut committed = false;
	for _ in 0..10_000 {
		if driver.step() == DriverProgress::Committed {
			committed = true;
			break;
		}
	}
	assert!(committed, "the driver never rebuilt a filter that had filled past its trigger");

	let after = filter.metrics();
	assert!(
		after.size_bits < armed_bits,
		"the rebuild kept the armed allocation instead of sizing from the source: {armed_bits} -> {}",
		after.size_bits
	);
	assert_eq!(after.rebuilds, 1);
	assert!(after.fill_ratio < filled, "the resized filter is no emptier than the saturated one it replaced");
	for n in 0..64u64 {
		assert!(
			filter.may_contain((EntryKind::Multi, &key(n))),
			"live row {n} was lost by the resizing rebuild"
		);
	}
}

#[test]
fn an_armed_store_filter_is_allocated_for_the_armed_capacity_and_starts_empty() {
	// size_bits is what the rebuild driver's fill trigger divides into, so an armed filter has to report a
	// real allocation rather than the zero a disabled filter reports; an empty fill pins that arming sets no
	// bits and therefore rules out every key it was never told about.
	let (storage, _guard) = SqlitePersistentStorage::in_memory();

	let metrics = storage.filter().metrics();
	assert!(metrics.enabled);
	assert_eq!(metrics.size_bits, ARMED_CAPACITY_KEYS * 10, "the armed filter was not sized for its capacity");
	assert_eq!(metrics.fill_ratio, 0.0);
	assert_eq!(metrics.estimated_keys, 0);
	assert_eq!(metrics.rebuilds, 0, "arming must not be recorded as a rebuild");
}

#[test]
fn the_keyset_scan_covers_every_table_exactly_once_on_an_exact_budget_multiple() {
	// Keyset pagination advances the cursor to the last key of the previous slice, so an off-by-one either
	// re-emits that key or skips the one after it, and the cursor has to reset when the walk crosses into
	// the next table. Twelve rows split over two tables at a budget of four is the case that hides the
	// classic bug: the scan cannot learn it is done until a slice comes back short.
	use reifydb_filter::source::KeyFilterSource;

	let (storage, _guard) = SqlitePersistentStorage::in_memory();
	let mut expected: Vec<u64> = Vec::new();
	for n in 0..6u64 {
		storage.set_collecting_accepted(
			CommitVersion(n + 1),
			batch(EntryKind::Multi, vec![(key(n), Some(body(n)))]),
		)
		.expect("setup write failed");
		storage.set_collecting_accepted(CommitVersion(n + 1), batch(OTHER, vec![(key(n), Some(body(n)))]))
			.expect("setup write failed");
		expected.push(hash_item(&(EntryKind::Multi, key(n).as_slice())));
		expected.push(hash_item(&(OTHER, key(n).as_slice())));
	}

	let mut source = MultiCurrentKeySource::new(storage.clone());
	assert_eq!(source.estimated_len(), 12, "the count must report every row the scan has to cover");

	let mut seen = Vec::new();
	let mut slices = 0;
	loop {
		let slice = source.next_slice(4);
		seen.extend(slice.hashes.iter().copied());
		slices += 1;
		if slice.exhausted {
			break;
		}
		assert!(slices < 100, "the keyset scan did not terminate");
	}

	let mut sorted_seen = seen.clone();
	sorted_seen.sort_unstable();
	sorted_seen.dedup();
	assert_eq!(seen.len(), 12, "the scan emitted {} hashes for 12 rows", seen.len());
	assert_eq!(sorted_seen.len(), 12, "the scan emitted a row twice across a slice or table boundary");
	let mut sorted_expected = expected.clone();
	sorted_expected.sort_unstable();
	assert_eq!(sorted_seen, sorted_expected, "the scan did not cover exactly the rows in the tables");

	source.restart();
	let mut again = Vec::new();
	loop {
		let slice = source.next_slice(5);
		again.extend(slice.hashes.iter().copied());
		if slice.exhausted {
			break;
		}
	}
	again.sort_unstable();
	assert_eq!(again, sorted_expected, "a restarted scan must replay every row from the beginning");
}
