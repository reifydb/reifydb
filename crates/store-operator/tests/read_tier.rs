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
use reifydb_sqlite::SqliteTempPathGuard;
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
fn a_filter_negative_lookup_leaves_the_tier_empty() {
	// The filter answers these in memory with no i/o, so caching them must never spend the byte budget.
	let (store, _storage, _guard) = cached_store();

	assert!(store.get(OP_A, &key(1)).is_none());
	assert!(!store.contains(OP_A, &key(1)));

	assert_eq!(entries(&store), 0, "a filter negative must never populate the tier");
	let tier = store.read().expect("the fixture configures a read tier");
	assert_eq!(tier.hits(), 0, "a filter negative is not a cache hit either");
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
		"the flush feeds the tier every row it persists, so eight ticks must need no persistent lookup at all; a fill per tick means each flush threw away the row it had just written"
	);
	assert_eq!(
		counters.hits, 7,
		"only the first read predates any write; the other seven must be answered by the tier, or the zero above is passing because nothing ever consulted it"
	);
	assert_eq!(counters.misses, 1, "the pre-write read is the only one the tier cannot answer");
	assert_eq!(body(&store.get(OP_A, &key(1)).expect("the key survives every cycle")), "0-1-2-3-4-5-6-7");
}
