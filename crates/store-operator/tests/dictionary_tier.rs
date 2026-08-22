// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Store-level wiring of the operator dictionary tier. The tier owns the group-interning keyspace outright, so
//! a key in it must be answered and remembered here and never offered to the read buffer, and the two caches
//! must not both hold the same key. The freshness contract is the read buffer's: a write invalidates, and the
//! flush writes the row it made durable back through.

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	sqlite::SqliteOperatorStorage,
	store::OperatorStore,
	tier::{
		dictionary::{OperatorDictionaryConfig, OperatorDictionaryTier},
		persistent::OperatorPersistentTier,
		read::{OperatorReadBufferConfig, OperatorReadBufferTier},
	},
};
use reifydb_value::value::duration::Duration;

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP: GroupId = GroupId(7);

fn cached_store() -> (OperatorStore, SqliteOperatorStorage, SqliteTempPathGuard) {
	// The hour-long interval on a frozen clock means the only drain a test sees is the one it asked for.
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage.clone()))
			.flush_interval(Duration::from_hours_const(1))),
		read: Some(OperatorReadBufferConfig::default()),
		dictionary: Some(OperatorDictionaryConfig::default()),
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn dictionary_key(suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::GROUP_DICTIONARY, [suffix]).as_encoded().clone()
}

fn other_key(suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, Keyspace::ACCUMULATOR, [suffix]).as_encoded().clone()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn dictionary(store: &OperatorStore) -> &OperatorDictionaryTier {
	store.dictionary().expect("the fixture configures a dictionary tier")
}

fn buffer(store: &OperatorStore) -> &OperatorReadBufferTier {
	store.read().expect("the fixture configures a read tier")
}

#[test]
fn a_repeated_dictionary_read_reaches_the_persistent_tier_once() {
	// One fill per key is the whole point; a dictionary the tier cannot hold sends every repeat read to sqlite.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, dictionary_key(1), row("durable"));

	for _ in 0..8 {
		let found = store.get(OP_A, &dictionary_key(1)).expect("the seeded row is readable");
		assert_eq!(body(&found), "durable", "every read must answer with the row sqlite holds");
	}

	let counters = dictionary(&store).metrics();
	assert_eq!(counters.fills_started, 1, "only the first read may reach the persistent tier");
	assert_eq!(counters.misses, 1, "only the first read may miss");
	assert_eq!(counters.hits, 7, "every later read must be served from the tier");
	assert_eq!(counters.fills_dirty_aborted, 0, "nothing wrote the key, so no fill may be discarded");
	assert_eq!(dictionary(&store).entries(), 1);
}

#[test]
fn the_read_buffer_never_caches_a_dictionary_key() {
	// Both tiers holding one key gives it two lifetimes and two invalidation paths, so one of them serves stale.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, dictionary_key(1), row("dict"));
	storage.set(OP_A, other_key(1), row("acc"));

	assert!(store.get(OP_A, &dictionary_key(1)).is_some());
	assert!(store.contains(OP_A, &dictionary_key(1)));
	assert!(store.get(OP_A, &other_key(1)).is_some());

	assert_eq!(buffer(&store).entries(), 1, "the read buffer must hold the accumulator key and nothing else");
	assert_eq!(buffer(&store).buckets(), 1, "a dictionary key must not open a bucket in the read buffer");
	assert_eq!(
		buffer(&store).metrics().fills_started,
		1,
		"the read buffer must never start a fill for a key the dictionary tier owns"
	);
	assert_eq!(dictionary(&store).entries(), 1, "the dictionary key must live in the dictionary tier instead");
}

#[test]
fn a_flushed_dictionary_write_is_served_fresh() {
	// Without invalidate-on-write the drained shadow uncovers the pre-write row and it is served forever.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, dictionary_key(1), row("old"));

	assert_eq!(body(&store.get(OP_A, &dictionary_key(1)).expect("the seeded row is readable")), "old");
	assert_eq!(dictionary(&store).entries(), 1, "the first read must cache, or this test proves nothing");

	store.set(OP_A, dictionary_key(1), row("new"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite before the staleness is observable");

	let found = store.get(OP_A, &dictionary_key(1)).expect("the key still exists after the flush");
	assert_eq!(body(&found), "new", "a cached row that survives its own overwrite is a value no writer wrote");
}

#[test]
fn a_flushed_dictionary_removal_is_not_served_from_the_tier() {
	// The removal only becomes visible at the flush, so the cached row must not outlive it.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, dictionary_key(1), row("gone"));

	assert!(store.get(OP_A, &dictionary_key(1)).is_some(), "the first read caches the row");

	store.remove(OP_A, &dictionary_key(1));
	assert!(store.flush_pending_blocking(), "the removal must reach sqlite");

	assert!(store.get(OP_A, &dictionary_key(1)).is_none(), "a removed key must never be served from the cache");
	assert!(!store.contains(OP_A, &dictionary_key(1)));
}

#[test]
fn dropping_operator_state_clears_only_that_operators_dictionary() {
	// A dropped operator's rows are gone from sqlite, so any surviving entry serves rows that no longer exist.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, dictionary_key(1), row("a"));
	storage.set(OP_B, dictionary_key(1), row("b"));

	assert!(store.get(OP_A, &dictionary_key(1)).is_some());
	assert!(store.get(OP_B, &dictionary_key(1)).is_some());
	assert_eq!(dictionary(&store).entries(), 2, "both operators must be cached before the drop");

	store.drop_operator_state(OP_A);

	assert_eq!(dictionary(&store).entries(), 1, "the dropped operator must leave no cached state behind");
	assert!(store.get(OP_A, &dictionary_key(1)).is_none(), "a dropped operator must read as gone");
	let survivor = store.get(OP_B, &dictionary_key(1)).expect("the other operator is untouched");
	assert_eq!(body(&survivor), "b", "dropping one operator must not collect another's identical key");
}
