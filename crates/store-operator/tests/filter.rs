// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The key filter in front of the persistent tier. A false positive costs one wasted sqlite read; a false
//! negative makes the store answer "no such row" for a row that exists, which is silent data loss. Every test
//! here aims at the false-negative direction. The rebuild is driven by stepping a RebuildDriver rather than by
//! waiting on the maintenance actor, because the driver is synchronous and the actor is not.

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
	util::bloom::hash_item,
};
use reifydb_filter::{
	config::FilterConfig,
	driver::{DriverProgress, RebuildDriver},
	source::KeyFilterSource,
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	filter::{OperatorKeyFilter, source::OperatorStateKeySource},
	sqlite::SqliteOperatorStorage,
	store::OperatorStore,
	tier::read::OperatorReadBufferConfig,
};
use reifydb_value::value::duration::Duration;

const OP: OperatorId = OperatorId(1);

const OTHER: OperatorId = OperatorId(2);

const GROUP: GroupId = GroupId(1);

fn key(suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, Keyspace::ACCUMULATOR, [suffix]).as_encoded().clone()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn store_at(config: SqliteConfig) -> OperatorStore {
	// an hour-long flush interval keeps the commit buffer out of the way; every row a test wants in sqlite
	// is written straight through the persistent tier instead
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::sqlite(config).flush_interval(Duration::from_hours_const(1))),
		read: Some(OperatorReadBufferConfig::default()),
		spawner,
		clock,
	})
}

fn rebuilding_config(scan_budget: usize) -> FilterConfig {
	// fill_trigger 0.0 makes every evaluate() start a rebuild, so a test can force a second pass over an
	// already enabled filter; the tight sizing keeps the false-positive rate low enough that a deleted key
	// answering present is a real defect rather than a collision.
	FilterConfig {
		scan_budget,
		size_headroom: 2.0,
		min_size_keys: 1,
		fill_trigger: 0.0,
		drift_trigger: 1.0,
		..FilterConfig::default()
	}
}

fn rebuild(storage: &SqliteOperatorStorage, config: FilterConfig) {
	// Drives one full cycle to the swap. The cap turns a driver that decides Idle, or never exhausts the
	// source, into a failure rather than a hang.
	let mut driver = RebuildDriver::new(
		storage.filter().handle(),
		Box::new(OperatorStateKeySource::new(storage.clone())),
		config,
	);
	for _ in 0..10_000 {
		if driver.step() == DriverProgress::Committed {
			return;
		}
	}
	panic!("filter rebuild never committed");
}

fn drain(source: &mut dyn KeyFilterSource, budget: usize) -> (Vec<u64>, Vec<bool>) {
	// Records the exhausted flag of every slice so a caller can assert it was raised exactly once, on the
	// slice after the last full one.
	let mut seen = Vec::new();
	let mut flags = Vec::new();
	loop {
		let slice = source.next_slice(budget);
		seen.extend(slice.hashes.iter().copied());
		flags.push(slice.exhausted);
		if slice.exhausted {
			return (seen, flags);
		}
		assert!(flags.len() < 100, "keyset scan did not terminate");
	}
}

#[test]
fn a_store_opened_on_existing_rows_serves_every_one_before_any_rebuild_has_run() {
	// The whole safety argument for starting disabled. A brand new filter holds no keys, so if it were
	// active it would answer "definitely absent" for every row already in sqlite and the store would report
	// an empty operator state after a restart. The fresh-filter assertions below are what pin the
	// disabled-and-permissive start; the reopened store then proves the read path really is permissive.
	let fresh = OperatorKeyFilter::new();
	assert!(!fresh.metrics().enabled, "a fresh filter must not be active, it holds none of the durable keys");
	assert!(fresh.may_contain(OP, &key(1)), "a disabled filter must answer may-contain for every key");

	let (config, _guard) = SqliteConfig::in_memory();
	{
		let storage = SqliteOperatorStorage::new(config.clone());
		for suffix in 0..32u8 {
			storage.set(OP, key(suffix), row(&format!("v{suffix}")));
		}
	}

	let booted = store_at(config);

	for suffix in 0..32u8 {
		let found = booted.get(OP, &key(suffix)).unwrap_or_else(|| {
			panic!("row {suffix} existed in sqlite but the freshly opened store reported it absent")
		});
		assert_eq!(
			String::from_utf8(found.body().to_vec()).expect("test bodies are utf8"),
			format!("v{suffix}")
		);
	}
}

#[test]
fn every_pre_existing_key_still_reads_back_after_a_completed_rebuild() {
	// The swap replaces a permissive filter with a restrictive one, so this is the moment a key can be
	// lost: any row the scan missed answers absent from then on, and the store never reads sqlite for it
	// again. Writes here go through the persistent tier while the filter is disabled, so add() is a no-op
	// and the only thing that can put a key into the new filter is the scan.
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	for suffix in 0..64u8 {
		storage.set(OP, key(suffix), row("live"));
		storage.set(OTHER, key(suffix), row("live"));
	}

	rebuild(&storage, rebuilding_config(7));

	assert!(storage.filter().metrics().enabled, "a committed rebuild must leave the filter active");
	for suffix in 0..64u8 {
		assert!(
			storage.filter().may_contain(OP, &key(suffix)),
			"operator 1 key {suffix} was lost by the rebuild"
		);
		assert!(
			storage.filter().may_contain(OTHER, &key(suffix)),
			"operator 2 key {suffix} was lost by the rebuild"
		);
		assert!(storage.get(OP, &key(suffix)).is_some(), "operator 1 row {suffix} became unreadable");
		assert!(storage.get(OTHER, &key(suffix)).is_some(), "operator 2 row {suffix} became unreadable");
	}
}

#[test]
fn the_source_hashes_a_key_exactly_as_the_read_path_does() {
	// The one invariant the whole port rests on: the rebuild source and the read/write path must hash the
	// same (operator, key) pair the same way. If they ever diverge, the rebuilt filter answers absent for
	// keys that exist and the store silently loses rows. The literal comparison catches a change to the
	// hashed expression; the may_contain round trip catches the two paths drifting apart at all.
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	storage.set(OP, key(3), row("written through the normal path"));

	let mut source = OperatorStateKeySource::new(storage.clone());
	let slice = source.next_slice(64);

	assert_eq!(slice.hashes.len(), 1, "the source must yield the one key that was written");
	assert_eq!(
		slice.hashes[0],
		hash_item(&(OP.0, key(3).as_slice())),
		"the source no longer hashes the pair the read path hashes"
	);

	rebuild(&storage, rebuilding_config(64));
	assert!(
		storage.filter().may_contain(OP, &key(3)),
		"a filter built purely from the source's hashes answered absent for a key the read path would test"
	);
}

#[test]
fn a_key_deleted_from_sqlite_stops_reporting_present_after_a_rebuild() {
	// The reason the rebuild exists: a bloom cannot delete, so without a rebuild every removed key stays a
	// permanent false positive and the filter degrades toward answering "present" for everything. The
	// surviving neighbours are asserted alongside so a rebuild that simply lost keys cannot pass.
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	for suffix in 0..128u8 {
		storage.set(OP, key(suffix), row("live"));
	}
	rebuild(&storage, rebuilding_config(32));
	for suffix in 0..16u8 {
		assert!(storage.filter().may_contain(OP, &key(suffix)), "setup failed: key {suffix} was never present");
	}

	for suffix in 0..16u8 {
		storage.remove(OP, &key(suffix));
	}
	rebuild(&storage, rebuilding_config(32));

	for suffix in 0..16u8 {
		assert!(
			!storage.filter().may_contain(OP, &key(suffix)),
			"deleted key {suffix} still reports present, so the rebuild is not reclaiming removed keys"
		);
	}
	for suffix in 16..128u8 {
		assert!(
			storage.filter().may_contain(OP, &key(suffix)),
			"surviving key {suffix} was dropped by the rebuild that reclaimed its deleted neighbours"
		);
	}
}

#[test]
fn a_reopened_populated_database_serves_rows_the_way_the_open_scan_used_to_establish() {
	// state_written gates every persistent read: while it is false, get/contains/range_batch short circuit
	// to "nothing here" without touching sqlite. The deleted open scan used to set it as a side effect of
	// finding any row, so a reopened database that skipped the existence probe would report every operator
	// as empty even though its rows are still on disk.
	let (config, _guard) = SqliteConfig::in_memory();
	{
		let storage = SqliteOperatorStorage::new(config.clone());
		storage.set(OP, key(1), row("durable"));
	}

	let reopened = SqliteOperatorStorage::new(config);

	let found = reopened.get(OP, &key(1)).expect("a reopened database must serve the rows it already holds");
	assert_eq!(String::from_utf8(found.body().to_vec()).expect("test bodies are utf8"), "durable");
	assert!(reopened.contains(OP, &key(1)), "contains must agree with get on a reopened database");
}

#[test]
fn the_keyset_scan_yields_every_row_exactly_once_including_on_an_exact_budget_multiple() {
	// Keyset pagination advances the cursor to the last row of the previous slice, so an off-by-one either
	// re-emits that row or skips the one after it. Twelve rows at a budget of four is the case that hides
	// the classic bug: the third slice comes back full, so the scan cannot stop there and must take one
	// more, empty, slice to learn it is done. Two operators make the row-value cursor cross an operator
	// boundary rather than only comparing keys.
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let mut expected: Vec<u64> = Vec::new();
	for suffix in 0..6u8 {
		storage.set(OP, key(suffix), row("x"));
		storage.set(OTHER, key(suffix), row("x"));
		expected.push(hash_item(&(OP.0, key(suffix).as_slice())));
		expected.push(hash_item(&(OTHER.0, key(suffix).as_slice())));
	}

	let mut source = OperatorStateKeySource::new(storage.clone());
	assert_eq!(source.estimated_len(), 12, "the census sum must report every row the scan has to cover");

	let (seen, flags) = drain(&mut source, 4);

	assert_eq!(
		flags,
		vec![false, false, false, true],
		"an exact multiple of the budget needs one final empty slice"
	);
	assert_eq!(seen.len(), 12, "the scan emitted {} hashes for 12 rows", seen.len());
	let mut sorted_seen = seen.clone();
	sorted_seen.sort_unstable();
	sorted_seen.dedup();
	assert_eq!(sorted_seen.len(), 12, "the scan emitted a row twice across a slice boundary");
	let mut sorted_expected = expected.clone();
	sorted_expected.sort_unstable();
	assert_eq!(sorted_seen, sorted_expected, "the scan did not cover exactly the rows in the table");

	source.restart();
	let (again, _) = drain(&mut source, 5);
	let mut sorted_again = again;
	sorted_again.sort_unstable();
	assert_eq!(sorted_again, sorted_expected, "a restarted scan must replay every row from the beginning");
}
