// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Transparency of the operator read buffer tier. The tier caches point reads of the persistent tier,
//! absences included, and the commit buffer only shadows a key until the flush drains it. A write therefore
//! invalidates the entry, because the row it recorded is not durable yet and a stale entry would outlive the
//! shadow and be served forever; the flush then writes the rows it just made durable back through, because
//! dropping them would leave the tier empty at the exact moment reads start reaching it.

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	sqlite::SqliteOperatorStorage,
	store::OperatorStore,
	tier::{persistent::OperatorPersistentTier, read::OperatorReadBufferConfig},
	types::OperatorWrite,
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
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn cached_store_on(storage: SqliteOperatorStorage) -> OperatorStore {
	// The same wiring as cached_store, but over a storage handle the caller already opened, so a test can seed
	// sqlite first and then boot a store that finds those rows already durable.
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage))
			.flush_interval(Duration::from_hours_const(1))),
		read: Some(OperatorReadBufferConfig::default()),
		spawner,
		clock,
	})
}

fn key(suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, Keyspace::ACCUMULATOR, [suffix]).as_encoded().clone()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn entries(store: &OperatorStore) -> usize {
	store.read().expect("the fixture configures a read tier").entries()
}

#[test]
fn a_flushed_write_is_served_fresh_after_the_read_tier_cached_the_previous_row() {
	// Without invalidate-on-write the drained shadow uncovers the pre-write row and it is served forever.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, key(1), row("durable"));

	let primed = store.get(OP_A, &key(1)).expect("the seeded row is readable");
	assert_eq!(
		body(&primed),
		"durable",
		"the first read must come from the persistent tier and populate the cache"
	);

	store.set(OP_A, key(1), row("fresh"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite before the staleness is observable");

	let found = store.get(OP_A, &key(1)).expect("the key still exists after the flush");
	assert_eq!(
		body(&found),
		"fresh",
		"a cached row that survives its own overwrite makes the store serve a value no writer ever wrote"
	);
	assert!(store.contains(OP_A, &key(1)));
}

#[test]
fn a_cached_absence_does_not_survive_the_write_that_fills_the_key() {
	// A remembered absence answers as a hit, so it must never outlive the write that fills the key.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, key(1), row("seed"));
	storage.remove(OP_A, &key(1));

	assert!(store.get(OP_A, &key(1)).is_none(), "the key is gone from sqlite but still passes the filter");
	assert_eq!(entries(&store), 1, "the absence itself must be cached, otherwise this test proves nothing");

	store.set(OP_A, key(1), row("written"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite before the staleness is observable");

	let found = store.get(OP_A, &key(1)).expect("the written row must be visible after the flush");
	assert_eq!(body(&found), "written", "a cached absence that outlives the write silently drops the row");
	assert!(
		store.contains(OP_A, &key(1)),
		"contains must agree, otherwise a branch on the key takes the wrong arm"
	);
}

#[test]
fn repeated_reads_of_one_key_cost_a_single_persistent_lookup() {
	// One fill for many reads is the whole point of the tier; a second fill means it never retained the row.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, key(1), row("durable"));

	for _ in 0..8 {
		assert_eq!(body(&store.get(OP_A, &key(1)).expect("the seeded row stays readable")), "durable");
	}

	let counters = store.read().expect("the fixture configures a read tier").metrics();
	assert_eq!(
		counters.fills_started, 1,
		"eight reads of one immutable row must cost exactly one persistent lookup"
	);
	assert_eq!(counters.misses, 1, "only the first read may miss");
	assert_eq!(counters.hits, 7, "every later read must be served from the tier");
	assert_eq!(counters.fills_dirty_aborted, 0, "nothing wrote the key, so no fill may be discarded");
}

#[test]
fn dropping_one_operators_state_clears_only_its_cached_entries() {
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, key(1), row("a-durable"));
	storage.set(OP_B, key(1), row("b-durable"));

	assert!(store.get(OP_A, &key(1)).is_some());
	assert!(store.get(OP_B, &key(1)).is_some());
	assert_eq!(entries(&store), 2, "both reads must have populated the tier before the drop is meaningful");

	store.drop_operator_state(OP_A);

	assert_eq!(entries(&store), 1, "the drop must evict the dropped operator's entries, not merely mask them");
	assert!(store.get(OP_A, &key(1)).is_none(), "the drop marker masks the rows sqlite still holds");

	let neighbour = store.get(OP_B, &key(1)).expect("a neighbour operator keeps its state across the drop");
	assert_eq!(
		body(&neighbour),
		"b-durable",
		"evicting by operator must be scoped; clearing the whole tier here throws away every other \
		 operator's warmed reads on each teardown"
	);

	assert!(store.flush_pending_blocking(), "the drop must reach sqlite");
	assert!(store.get(OP_A, &key(1)).is_none(), "the erased rows must stay gone once the mask is drained");
	assert_eq!(body(&store.get(OP_B, &key(1)).expect("the neighbour survives the flushed drop")), "b-durable");
}

#[test]
fn a_lookup_under_a_disabled_filter_caches_the_absence() {
	// A store reopened on rows it already holds must start with a disabled filter, because an armed empty bloom
	// would answer absent for every durable row. Nothing is ruled out until the background rebuild lands, so
	// through that whole window the absence has to be bought from sqlite once and then remembered; a second miss
	// here means every lookup for a key that does not exist pays a persistent read for as long as it lasts.
	let (config, _guard) = SqliteConfig::in_memory();
	{
		let seed = SqliteOperatorStorage::new(config.clone());
		seed.set(OP_B, key(9), row("seed"));
	}

	let store = cached_store_on(SqliteOperatorStorage::new(config));
	let metrics = store.persistent().expect("the fixture configures a persistent tier").filter().metrics();
	assert!(!metrics.enabled, "a reopen over durable rows must not arm the filter, or those rows read back absent");

	assert!(store.get(OP_A, &key(1)).is_none());
	assert!(!store.contains(OP_A, &key(1)));

	assert_eq!(entries(&store), 1, "the tier must remember the absence it paid sqlite to learn");
	let tier = store.read().expect("the fixture configures a read tier");
	assert_eq!(tier.hits(), 1, "the repeat lookup must be served by the remembered absence, not by sqlite again");
}

#[test]
fn a_memory_only_store_builds_no_read_tier() {
	// Without a persistent tier the commit buffer never drains, so a cache of it could never be hit.
	assert!(OperatorStore::testing_memory().read().is_none(), "the memory tier must not carry a read cache");

	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let store = OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: None,
		read: Some(OperatorReadBufferConfig::default()),
		spawner: actor_system.spawner(),
		clock,
	});

	assert!(
		store.read().is_none(),
		"an explicit read config without a persistent tier must still build nothing; honouring it would \
		 reserve a budget no read can ever use"
	);
}

#[test]
fn contains_is_invalidated_by_a_write_exactly_as_get_is() {
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, key(1), row("durable"));

	assert!(store.contains(OP_A, &key(1)), "the first contains populates the tier from sqlite");
	assert_eq!(entries(&store), 1);

	store.remove(OP_A, &key(1));
	assert!(store.flush_pending_blocking(), "the removal must reach sqlite before the staleness is observable");

	assert!(
		!store.contains(OP_A, &key(1)),
		"a cached presence that outlives the removal makes an operator branch on state it just deleted"
	);
	assert!(store.get(OP_A, &key(1)).is_none(), "get must agree with contains on the same key");

	store.set(OP_A, key(1), row("again"));
	assert!(store.flush_pending_blocking(), "the rewrite must reach sqlite too");

	assert!(store.contains(OP_A, &key(1)), "and the cached absence must not outlive the rewrite either");
}

#[test]
fn a_batch_write_invalidates_every_state_key_it_carries() {
	// The bulk path must invalidate exactly like the point path, or it leaves the same stale row unseen.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, key(1), row("durable-1"));
	storage.set(OP_A, key(2), row("durable-2"));
	storage.set(OP_A, key(3), row("durable-3"));

	for suffix in 1u8..=3 {
		assert!(store.get(OP_A, &key(suffix)).is_some());
	}
	assert_eq!(entries(&store), 3);

	store.apply_batch(&[
		OperatorWrite::Set {
			operator: OP_A,
			key: key(1),
			row: row("batched"),
		},
		OperatorWrite::Remove {
			operator: OP_A,
			key: key(2),
		},
	]);
	assert!(store.flush_pending_blocking(), "the batch must reach sqlite before the staleness is observable");

	assert_eq!(
		body(&store.get(OP_A, &key(1)).expect("the batched write is durable")),
		"batched",
		"a batched set must invalidate its key just like a point set"
	);
	assert!(store.get(OP_A, &key(2)).is_none(), "a batched remove must invalidate its key too");
	assert_eq!(
		body(&store.get(OP_A, &key(3)).expect("an untouched key keeps its row")),
		"durable-3",
		"invalidation must be per key, otherwise one batch cold-starts the whole operator"
	);
}

#[test]
fn a_write_in_flight_is_still_shadowed_and_never_lets_the_tier_cache_the_old_row() {
	// A lookup that read only the live batch would fall through as absent here and cache the pre-write row.
	let (store, storage, _guard) = cached_store();
	storage.set(OP_A, key(1), row("durable"));

	store.set(OP_A, key(1), row("fresh"));
	let batch = store.commit().take_for_flush().expect("the write is pending and must be taken for flush");

	let found = store.get(OP_A, &key(1)).expect("the in-flight write is still the newest value for the key");
	assert_eq!(body(&found), "fresh", "a read during the flush window must see the in-flight write, not sqlite");
	assert_eq!(entries(&store), 0, "and it must not have cached the row sqlite still holds");

	drop(batch);
	store.commit().complete_flush();
}

#[test]
fn a_flush_leaves_the_row_it_persisted_cached_instead_of_forcing_a_refill() {
	// The commit buffer shadows a key only until the flush drains it, so a flush that drops the entry it just wrote
	// leaves the tier empty at the exact moment reads start reaching it; a key rewritten every tick then costs a
	// persistent lookup per tick and never hits.
	let (store, _storage, _guard) = cached_store();
	store.set(OP_A, key(1), row("written"));
	assert!(store.flush_pending_blocking(), "the shadow must be drained before reads reach the tier");

	let before = store.read().expect("the fixture configures a read tier").metrics();
	let found = store.get(OP_A, &key(1)).expect("the flushed row is readable");
	assert_eq!(body(&found), "written", "the tier must serve the row the flush made durable, not an older one");

	let after = store.read().expect("the fixture configures a read tier").metrics();
	assert_eq!(
		after.fills_started, before.fills_started,
		"the flush already held this row; refilling it from sqlite is the miss this path exists to avoid"
	);
	assert_eq!(after.hits, before.hits + 1, "the read must be answered by the tier, not merely skipped past it");
	assert_eq!(after.misses, before.misses, "a counted miss means the entry did not survive its own flush");
}

#[test]
fn a_read_modify_write_key_stays_cached_across_repeated_flush_cycles() {
	// Counters and accumulators are read then written every tick; if each cycle costs a fresh fill the tier is
	// permanently one step behind the writer and its hit rate for those keyspaces is pinned at zero.
	let (store, _storage, _guard) = cached_store();

	for tick in 0..8u8 {
		let seen = store.get(OP_A, &key(1));
		let next = match seen {
			Some(found) => format!("{}-{tick}", body(&found)),
			None => format!("{tick}"),
		};
		store.set(OP_A, key(1), row(&next));
		assert!(store.flush_pending_blocking(), "each tick must reach sqlite before the next read");
	}

	let counters = store.read().expect("the fixture configures a read tier").metrics();
	assert_eq!(
		counters.fills_started, 0,
		"the armed filter rules out the pre-write read and the flush feeds the tier every row it persists, so any fill at all means a flush threw away the row it had just written"
	);
	assert_eq!(
		counters.hits, 7,
		"only the first read predates any write; the other seven must be answered by the tier, or the zero above is passing because nothing ever consulted it"
	);
	assert_eq!(counters.misses, 1, "the pre-write read is the only one the tier cannot answer");
	assert_eq!(body(&store.get(OP_A, &key(1)).expect("the key survives every cycle")), "0-1-2-3-4-5-6-7");
}

#[test]
fn a_flushed_removal_leaves_no_entry_behind_for_the_key_it_erased() {
	// Retention erases rows by the million and never reads them again. Caching each erasure as an absence
	// charges the tier per-entry overhead plus the whole key for a lookup that will never come, and only
	// whole-bucket eviction ever takes it back, so the buffer fills with rows retention already deleted.
	let (store, _storage, _guard) = cached_store();
	store.set(OP_A, key(1), row("doomed"));
	assert!(store.flush_pending_blocking(), "the row must be durable before the removal can erase it");
	let seeded = entries(&store);
	assert_eq!(seeded, 1, "precondition: the flush left exactly the one row it persisted");

	store.remove(OP_A, &key(1));
	assert!(store.flush_pending_blocking(), "the removal must reach the tier through the same flush path");

	assert_eq!(
		entries(&store),
		0,
		"a flushed removal must drop the entry, not replace the row with a tombstone that outlives it"
	);
}

#[test]
fn a_flushed_removal_still_reads_back_as_absent() {
	// Dropping the entry must not make the key answer wrongly: the next read has to reach the persistent
	// tier and find nothing there, or the erase would be invisible and the stale row served forever.
	let (store, _storage, _guard) = cached_store();
	store.set(OP_A, key(1), row("doomed"));
	assert!(store.flush_pending_blocking());
	store.remove(OP_A, &key(1));
	assert!(store.flush_pending_blocking());

	assert_eq!(store.get(OP_A, &key(1)), None, "the erased key must not resurrect from the tier");
}

fn key_in(keyspace: Keyspace, suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, keyspace, [suffix]).as_encoded().clone()
}

#[test]
fn a_keyspace_declared_not_cached_never_occupies_the_read_buffer() {
	// These three keyspaces were measured holding 11.67 MiB of a 64 MiB budget while answering 0.097% of
	// hits. Every entry they take is an entry a hot keyspace loses to whole-bucket eviction, so admission
	// must refuse them outright; caching them under a different name is the failure this pins.
	for keyspace in [Keyspace::CUSTOM_NOT_CACHED, Keyspace::JOIN_PIN, Keyspace::ENGINE_META] {
		let (store, _storage, _guard) = cached_store();
		let key = key_in(keyspace, 1);
		store.set(OP_A, key.clone(), row("durable"));
		assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

		for _ in 0..4 {
			assert_eq!(
				body(&store.get(OP_A, &key).expect("a refused keyspace must still read correctly")),
				"durable",
				"{} bypasses the tier, it does not lose its rows",
				keyspace.name()
			);
		}

		assert_eq!(
			entries(&store),
			0,
			"{} must leave nothing behind: neither the flush write-through nor a read fill may admit it",
			keyspace.name()
		);
		assert_eq!(
			store.read().expect("the fixture configures a read tier").metrics().fills_started,
			0,
			"{} must be refused before the fill starts; a fill that can only be thrown away still takes the shard lock and inflates the counter",
			keyspace.name()
		);
	}
}

#[test]
fn a_keyspace_declared_cached_still_occupies_the_read_buffer() {
	// The control for the refusal above: a gate that refuses everything would pass that test while turning
	// the whole tier off, and the throughput loss would only show up in a replay.
	let (store, _storage, _guard) = cached_store();
	store.set(OP_A, key_in(Keyspace::CUSTOM_CACHED, 1), row("durable"));
	assert!(store.flush_pending_blocking());

	assert_eq!(entries(&store), 1, "a cached keyspace must be admitted, or the gate is a blanket off switch");
}

#[test]
fn a_refused_keyspace_reads_absent_without_remembering_the_absence() {
	// A refused keyspace must not be admitted through the absence path either: a miss that caches "nothing
	// here" costs the same entry overhead as a row and is exactly what the read fill would have written.
	let (store, storage, _guard) = cached_store();
	let key = key_in(Keyspace::JOIN_PIN, 1);
	storage.set(OP_A, key.clone(), row("seed"));
	storage.remove(OP_A, &key);

	assert_eq!(store.get(OP_A, &key), None, "the key is gone from sqlite but still passes the filter");
	assert_eq!(entries(&store), 0, "the absence must not be remembered for a refused keyspace");
}

#[test]
fn an_expiry_write_never_occupies_the_read_buffer() {
	// The expiry index is drained by due-ordered range scans and never point read, so every entry the flush
	// write-through admits is budget a read-served keyspace loses to whole-bucket eviction and can never win
	// back with a hit.
	let (store, _storage, _guard) = cached_store();
	let key = key_in(Keyspace::EXPIRY, 1);
	store.set(OP_A, key.clone(), row("armed"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

	for _ in 0..4 {
		assert_eq!(
			body(&store.get(OP_A, &key).expect("a refused keyspace must still read correctly")),
			"armed",
			"EXPIRY bypasses the tier, it does not lose its rows"
		);
	}

	assert_eq!(
		entries(&store),
		0,
		"EXPIRY must leave nothing behind: neither the flush write-through nor a read fill may admit it"
	);
	assert_eq!(
		store.read().expect("the fixture configures a read tier").metrics().fills_started,
		0,
		"EXPIRY must be refused before the fill starts; a fill that can only be thrown away still takes the shard lock"
	);
}

#[test]
fn a_timer_wheel_write_never_occupies_the_read_buffer() {
	// The timer wheel is read only by the due-ordered range scan that drains it, so admitting its rows buys
	// no hit and spends budget the keyspaces that are point read need.
	let (store, _storage, _guard) = cached_store();
	let key = key_in(Keyspace::TIMER_WHEEL, 1);
	store.set(OP_A, key.clone(), row("armed"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

	for _ in 0..4 {
		assert_eq!(
			body(&store.get(OP_A, &key).expect("a refused keyspace must still read correctly")),
			"armed",
			"TIMER_WHEEL bypasses the tier, it does not lose its rows"
		);
	}

	assert_eq!(
		entries(&store),
		0,
		"TIMER_WHEEL must leave nothing behind: neither the flush write-through nor a read fill may admit it"
	);
	assert_eq!(
		store.read().expect("the fixture configures a read tier").metrics().fills_started,
		0,
		"TIMER_WHEEL must be refused before the fill starts; a fill that can only be thrown away still takes the shard lock"
	);
}

#[test]
fn a_point_read_of_an_expiry_key_remembers_neither_the_row_nor_the_absence() {
	// The absence path costs the same entry overhead as a row, so a refused keyspace that still caches "nothing
	// here" gives back exactly the budget the refusal was meant to free.
	let (store, storage, _guard) = cached_store();
	let present = key_in(Keyspace::EXPIRY, 1);
	let absent = key_in(Keyspace::EXPIRY, 2);
	storage.set(OP_A, present.clone(), row("armed"));
	storage.set(OP_A, absent.clone(), row("seed"));
	storage.remove(OP_A, &absent);

	assert_eq!(body(&store.get(OP_A, &present).expect("the seeded row is readable")), "armed");
	assert_eq!(store.get(OP_A, &absent), None, "the key is gone from sqlite but still passes the filter");

	assert_eq!(entries(&store), 0, "neither the row nor the absence may be remembered for EXPIRY");
}

#[test]
fn a_row_number_mapping_write_still_occupies_the_read_buffer() {
	// The control for the two refusals above: a refusal list that widened past the keyspaces the measurement
	// shows serving hits would turn the tier into an off switch and only show up as a throughput loss in a replay.
	let (store, _storage, _guard) = cached_store();
	store.set(OP_A, key_in(Keyspace::ROW_NUMBER_MAPPING, 1), row("durable"));
	assert!(store.flush_pending_blocking());

	assert_eq!(entries(&store), 1, "a cached keyspace must still be admitted after the refusal list grew");
}
