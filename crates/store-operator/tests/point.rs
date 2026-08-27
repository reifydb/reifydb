// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Transparency of the operator point tier. The tier caches point reads of the persistent tier, absences
//! included, and the commit buffer only shadows a key until the flush drains it. A write therefore invalidates
//! the entry, because the row it recorded is not durable yet and a stale entry would outlive the shadow and be
//! served forever; the flush then writes the rows it just made durable back through, because dropping them
//! would leave the tier empty at the exact moment reads start reaching it. A point read consults the range tier
//! after this one, so a whole bucket is the authority on any key it covers and outranks a remembered absence.

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, keyspace_inner_range},
	metrics::scan::ScanCounters,
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_operator::{
	config::{OperatorCommitConfig, OperatorPersistentConfig, OperatorStoreConfig},
	sqlite::SqliteOperatorStorage,
	store::OperatorStore,
	tier::{
		commit::OperatorCommitBuffer,
		persistent::OperatorPersistentTier,
		point::{OperatorPointConfig, OperatorPointTier},
		range::{OperatorRangeConfig, OperatorRangeTier},
	},
	types::{DurablePre, OperatorBatch, OperatorWrite},
};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

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
		point: Some(OperatorPointConfig::testing()),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn cached_store_on(storage: SqliteOperatorStorage) -> OperatorStore {
	// The same wiring over a handle the caller already opened, so a test can boot a store on rows already durable.
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage))
			.flush_interval(Duration::from_hours_const(1))),
		point: Some(OperatorPointConfig::testing()),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock,
	})
}

fn key(suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, Keyspace::ACCUMULATOR, [suffix]).as_encoded().clone()
}

fn key_in(keyspace: Keyspace, suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, keyspace, [suffix]).as_encoded().clone()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn bodies(batch: &OperatorBatch) -> Vec<String> {
	batch.items.iter().map(|(_, row)| body(row)).collect()
}

fn accumulator_range() -> EncodedKeyRange {
	keyspace_inner_range(GROUP, Keyspace::ACCUMULATOR)
}

fn seed_accumulator(storage: &SqliteOperatorStorage, count: u8) {
	for suffix in 1..=count {
		storage.apply_batch(&[OperatorWrite::Insert {
			operator: OP_A,
			key: key_in(Keyspace::ACCUMULATOR, suffix),
			post: row(&format!("v{suffix}")),
		}]);
	}
}

fn point_tier(store: &OperatorStore) -> &OperatorPointTier {
	store.point().expect("the fixture configures a point tier")
}

fn range_tier(store: &OperatorStore) -> &OperatorRangeTier {
	store.range().expect("the fixture configures a range tier")
}

fn point_entries(store: &OperatorStore) -> usize {
	point_tier(store).entries()
}

fn range_entries(store: &OperatorStore) -> usize {
	range_tier(store).entries()
}

fn put(store: &OperatorStore, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
	// reading the pre-image back keeps the claim truthful even when an earlier write in the same test moved the key
	let write = match store.get(operator, &key) {
		Some(pre) => OperatorWrite::Replace {
			operator,
			key,
			pre_value_bytes: ByteSize::from_bytes(pre.bytes().len() as u64),
			post: row,
		},
		None => OperatorWrite::Insert {
			operator,
			key,
			post: row,
		},
	};
	store.apply_batch(&[write]);
}

fn erase(store: &OperatorStore, operator: OperatorId, key: &EncodedKey) {
	// a removal must say whether the key was there, and only the store knows after the writes above it
	let pre = match store.get(operator, key) {
		Some(row) => DurablePre::Present(ByteSize::from_bytes(row.bytes().len() as u64)),
		None => DurablePre::Absent,
	};
	store.apply_batch(&[OperatorWrite::Remove {
		operator,
		key: key.clone(),
		pre,
	}]);
}

#[test]
fn a_flushed_write_is_served_fresh_after_the_point_tier_cached_the_previous_row() {
	// Without invalidate-on-write the drained shadow uncovers the pre-write row and it is served forever.
	let (store, storage, _guard) = cached_store();
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("durable"),
	}]);

	let primed = store.get(OP_A, &key(1)).expect("the seeded row is readable");
	assert_eq!(
		body(&primed),
		"durable",
		"the first read must come from the persistent tier and populate the cache"
	);

	put(&store, OP_A, key(1), row("fresh"));
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
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("seed"),
	}]);
	storage.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: key(1),
		pre: DurablePre::Present(ByteSize::from_bytes(row("seed").bytes().len() as u64)),
	}]);

	assert!(store.get(OP_A, &key(1)).is_none(), "the key is gone from sqlite but still passes the filter");
	assert_eq!(point_entries(&store), 1, "the absence itself must be cached, otherwise this test proves nothing");

	put(&store, OP_A, key(1), row("written"));
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
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("durable"),
	}]);

	for _ in 0..8 {
		assert_eq!(body(&store.get(OP_A, &key(1)).expect("the seeded row stays readable")), "durable");
	}

	let counters = point_tier(&store).metrics();
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
	// Two operators sharing identical key bytes must not be collected together by a drop of either one.
	let (store, storage, _guard) = cached_store();
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("a-durable"),
	}]);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_B,
		key: key(1),
		post: row("b-durable"),
	}]);

	assert!(store.get(OP_A, &key(1)).is_some());
	assert!(store.get(OP_B, &key(1)).is_some());
	assert_eq!(point_entries(&store), 2, "both reads must have populated the tier before the drop is meaningful");

	store.drop_operator_state(OP_A);

	assert_eq!(
		point_entries(&store),
		1,
		"the drop must evict the dropped operator's entries, not merely mask them"
	);
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
	// While the bloom is disabled nothing is ruled out, so an absence must be bought from sqlite once and then
	// remembered, or every lookup of a missing key pays a persistent read for as long as the rebuild lasts.
	let (config, _guard) = SqliteConfig::in_memory();
	{
		let seed = SqliteOperatorStorage::new(config.clone());
		seed.apply_batch(&[OperatorWrite::Insert {
			operator: OP_B,
			key: key(9),
			post: row("seed"),
		}]);
	}

	let store = cached_store_on(SqliteOperatorStorage::new(config));
	let metrics = store.persistent().expect("the fixture configures a persistent tier").filter().metrics();
	assert!(!metrics.enabled, "a reopen over durable rows must not arm the filter, or those rows read back absent");

	assert!(store.get(OP_A, &key(1)).is_none());
	assert!(!store.contains(OP_A, &key(1)));

	assert_eq!(point_entries(&store), 1, "the tier must remember the absence it paid sqlite to learn");
	assert_eq!(
		point_tier(&store).hits(),
		1,
		"the repeat lookup must be served by the remembered absence, not by sqlite again"
	);
}

#[test]
fn a_memory_only_store_builds_no_read_caches() {
	// Without a persistent tier the commit buffer never drains, so neither cache could ever be hit and neither
	// budget may be reserved.
	let memory = OperatorStore::testing_memory();
	assert!(memory.point().is_none(), "the memory tier must not carry a point cache");
	assert!(memory.range().is_none(), "the memory tier must not carry a range cache");

	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let store = OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: None,
		point: Some(OperatorPointConfig::testing()),
		range: Some(OperatorRangeConfig::testing()),
		spawner: actor_system.spawner(),
		clock,
	});

	assert!(
		store.point().is_none(),
		"an explicit point config without a persistent tier must still build nothing; honouring it would \
		 reserve a budget no read can ever use"
	);
	assert!(
		store.range().is_none(),
		"and the range half must not be reserved either, or half the budget survives the assertion above"
	);
}

#[test]
fn contains_is_invalidated_by_a_write_exactly_as_get_is() {
	// contains and get read the same entry, so they must never disagree across a removal or a rewrite.
	let (store, storage, _guard) = cached_store();
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("durable"),
	}]);

	assert!(store.contains(OP_A, &key(1)), "the first contains populates the tier from sqlite");
	assert_eq!(point_entries(&store), 1);

	erase(&store, OP_A, &key(1));
	assert!(store.flush_pending_blocking(), "the removal must reach sqlite before the staleness is observable");

	assert!(
		!store.contains(OP_A, &key(1)),
		"a cached presence that outlives the removal makes an operator branch on state it just deleted"
	);
	assert!(store.get(OP_A, &key(1)).is_none(), "get must agree with contains on the same key");

	put(&store, OP_A, key(1), row("again"));
	assert!(store.flush_pending_blocking(), "the rewrite must reach sqlite too");

	assert!(store.contains(OP_A, &key(1)), "and the cached absence must not outlive the rewrite either");
}

#[test]
fn a_batch_write_invalidates_every_state_key_it_carries() {
	// The bulk path must invalidate exactly like the point path, or it leaves the same stale row unseen.
	let (store, storage, _guard) = cached_store();
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("durable-1"),
	}]);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(2),
		post: row("durable-2"),
	}]);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(3),
		post: row("durable-3"),
	}]);

	for suffix in 1u8..=3 {
		assert!(store.get(OP_A, &key(suffix)).is_some());
	}
	assert_eq!(point_entries(&store), 3);

	store.apply_batch(&[
		OperatorWrite::Replace {
			operator: OP_A,
			key: key(1),
			pre_value_bytes: ByteSize::from_bytes(row("durable-1").bytes().len() as u64),
			post: row("batched"),
		},
		OperatorWrite::Remove {
			operator: OP_A,
			key: key(2),
			pre: DurablePre::Present(ByteSize::from_bytes(row("durable-2").bytes().len() as u64)),
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
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("durable"),
	}]);

	put(&store, OP_A, key(1), row("fresh"));
	let batch = store.commit().take_for_flush().expect("the write is pending and must be taken for flush");

	let found = store.get(OP_A, &key(1)).expect("the in-flight write is still the newest value for the key");
	assert_eq!(body(&found), "fresh", "a read during the flush window must see the in-flight write, not sqlite");
	assert_eq!(point_entries(&store), 0, "and it must not have cached the row sqlite still holds");

	drop(batch);
	store.commit().complete_flush();
}

#[test]
fn a_flush_leaves_the_row_it_persisted_cached_instead_of_forcing_a_refill() {
	// The shadow lifts at the flush, so a flush that drops the row it just wrote makes a per-tick key pay a
	// persistent lookup every tick and never hit.
	let (store, _storage, _guard) = cached_store();
	put(&store, OP_A, key(1), row("written"));
	assert!(store.flush_pending_blocking(), "the shadow must be drained before reads reach the tier");

	let before = point_tier(&store).metrics();
	let found = store.get(OP_A, &key(1)).expect("the flushed row is readable");
	assert_eq!(body(&found), "written", "the tier must serve the row the flush made durable, not an older one");

	let after = point_tier(&store).metrics();
	assert_eq!(
		after.fills_started, before.fills_started,
		"the flush already held this row; refilling it from sqlite is the miss this path exists to avoid"
	);
	assert_eq!(after.hits, before.hits + 1, "the read must be answered by the tier, not merely skipped past it");
	assert_eq!(after.misses, before.misses, "a counted miss means the entry did not survive its own flush");
}

#[test]
fn a_read_modify_write_key_stays_cached_across_repeated_flush_cycles() {
	// Counters are read then written every tick, so a fresh fill per cycle pins the hit rate of those keyspaces at
	// zero.
	let (store, _storage, _guard) = cached_store();

	for tick in 0..8u8 {
		// the tick's own read is the pre-image, so the claim costs no extra counted read of its own
		let seen = store.get(OP_A, &key(1));
		let next = match &seen {
			Some(found) => format!("{}-{tick}", body(found)),
			None => format!("{tick}"),
		};
		let write = match &seen {
			Some(pre) => OperatorWrite::Replace {
				operator: OP_A,
				key: key(1),
				pre_value_bytes: ByteSize::from_bytes(pre.bytes().len() as u64),
				post: row(&next),
			},
			None => OperatorWrite::Insert {
				operator: OP_A,
				key: key(1),
				post: row(&next),
			},
		};
		store.apply_batch(&[write]);
		assert!(store.flush_pending_blocking(), "each tick must reach sqlite before the next read");
	}

	let counters = point_tier(&store).metrics();
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
	// Retention erases rows by the million and never reads them again, so caching each erasure as an absence fills
	// the tier with keys no lookup will ever come for.
	let (store, _storage, _guard) = cached_store();
	put(&store, OP_A, key(1), row("doomed"));
	assert!(store.flush_pending_blocking(), "the row must be durable before the removal can erase it");
	let seeded = point_entries(&store);
	assert_eq!(seeded, 1, "precondition: the flush left exactly the one row it persisted");

	erase(&store, OP_A, &key(1));
	assert!(store.flush_pending_blocking(), "the removal must reach the tier through the same flush path");

	assert_eq!(
		point_entries(&store),
		0,
		"a flushed removal must drop the entry, not replace the row with a tombstone that outlives it"
	);
}

#[test]
fn a_flushed_removal_still_reads_back_as_absent() {
	// Dropping the entry must not make the key answer wrongly, or the erase is invisible and the stale row is
	// served forever.
	let (store, _storage, _guard) = cached_store();
	put(&store, OP_A, key(1), row("doomed"));
	assert!(store.flush_pending_blocking());
	erase(&store, OP_A, &key(1));
	assert!(store.flush_pending_blocking());

	assert_eq!(store.get(OP_A, &key(1)), None, "the erased key must not resurrect from the tier");
}

#[test]
fn a_keyspace_declared_not_cached_never_occupies_the_point_tier() {
	// These three were measured holding 11.67 MiB of a 64 MiB budget for 0.097% of hits, so every entry they take
	// is one a hot keyspace loses to eviction.
	for keyspace in [Keyspace::CUSTOM_NOT_CACHED, Keyspace::JOIN_PIN, Keyspace::ENGINE_META] {
		let (store, _storage, _guard) = cached_store();
		let key = key_in(keyspace, 1);
		put(&store, OP_A, key.clone(), row("durable"));
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
			point_entries(&store),
			0,
			"{} must leave nothing behind: neither the flush write-through nor a read fill may admit it",
			keyspace.name()
		);
		assert_eq!(
			point_tier(&store).metrics().fills_started,
			0,
			"{} must be refused before the fill starts; a fill that can only be thrown away still takes the shard lock and inflates the counter",
			keyspace.name()
		);
	}
}

#[test]
fn a_keyspace_declared_cached_still_occupies_the_point_tier() {
	// The control for the refusal above: a gate that refuses everything passes that test while turning the tier
	// off.
	let (store, _storage, _guard) = cached_store();
	put(&store, OP_A, key_in(Keyspace::CUSTOM_CACHED, 1), row("durable"));
	assert!(store.flush_pending_blocking());

	assert_eq!(point_entries(&store), 1, "a cached keyspace must be admitted, or the gate is a blanket off switch");
}

#[test]
fn a_refused_keyspace_reads_absent_without_remembering_the_absence() {
	// An absence costs the same entry overhead as a row, so the refusal must cover the absence path too.
	let (store, storage, _guard) = cached_store();
	let key = key_in(Keyspace::JOIN_PIN, 1);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key.clone(),
		post: row("seed"),
	}]);
	storage.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: key.clone(),
		pre: DurablePre::Present(ByteSize::from_bytes(row("seed").bytes().len() as u64)),
	}]);

	assert_eq!(store.get(OP_A, &key), None, "the key is gone from sqlite but still passes the filter");
	assert_eq!(point_entries(&store), 0, "the absence must not be remembered for a refused keyspace");
}

#[test]
fn an_expiry_write_never_occupies_the_point_tier() {
	// The expiry index is drained by range scans and never point read, so an entry the write-through admits can
	// never win its budget back with a hit.
	let (store, _storage, _guard) = cached_store();
	let key = key_in(Keyspace::EXPIRY, 1);
	put(&store, OP_A, key.clone(), row("armed"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

	for _ in 0..4 {
		assert_eq!(
			body(&store.get(OP_A, &key).expect("a refused keyspace must still read correctly")),
			"armed",
			"EXPIRY bypasses the tier, it does not lose its rows"
		);
	}

	assert_eq!(
		point_entries(&store),
		0,
		"EXPIRY must leave nothing behind: neither the flush write-through nor a read fill may admit it"
	);
	assert_eq!(
		point_tier(&store).metrics().fills_started,
		0,
		"EXPIRY must be refused before the fill starts; a fill that can only be thrown away still takes the shard lock"
	);
}

#[test]
fn a_timer_wheel_write_never_occupies_the_point_tier() {
	// The timer wheel is read only by the due-ordered scan that drains it, so admitting its rows buys no hit at
	// all.
	let (store, _storage, _guard) = cached_store();
	let key = key_in(Keyspace::TIMER_WHEEL, 1);
	put(&store, OP_A, key.clone(), row("armed"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

	for _ in 0..4 {
		assert_eq!(
			body(&store.get(OP_A, &key).expect("a refused keyspace must still read correctly")),
			"armed",
			"TIMER_WHEEL bypasses the tier, it does not lose its rows"
		);
	}

	assert_eq!(
		point_entries(&store),
		0,
		"TIMER_WHEEL must leave nothing behind: neither the flush write-through nor a read fill may admit it"
	);
	assert_eq!(
		point_tier(&store).metrics().fills_started,
		0,
		"TIMER_WHEEL must be refused before the fill starts; a fill that can only be thrown away still takes the shard lock"
	);
}

#[test]
fn a_point_read_of_an_expiry_key_remembers_neither_the_row_nor_the_absence() {
	// A refused keyspace that still caches "nothing here" gives back exactly the budget the refusal freed.
	let (store, storage, _guard) = cached_store();
	let present = key_in(Keyspace::EXPIRY, 1);
	let absent = key_in(Keyspace::EXPIRY, 2);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: present.clone(),
		post: row("armed"),
	}]);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: absent.clone(),
		post: row("seed"),
	}]);
	storage.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: absent.clone(),
		pre: DurablePre::Present(ByteSize::from_bytes(row("seed").bytes().len() as u64)),
	}]);

	assert_eq!(body(&store.get(OP_A, &present).expect("the seeded row is readable")), "armed");
	assert_eq!(store.get(OP_A, &absent), None, "the key is gone from sqlite but still passes the filter");

	assert_eq!(point_entries(&store), 0, "neither the row nor the absence may be remembered for EXPIRY");
}

#[test]
fn a_row_number_mapping_write_still_occupies_the_point_tier() {
	// The control for the two refusals above: a refusal list that widened past the measurement turns the tier into
	// an off switch.
	let (store, _storage, _guard) = cached_store();
	put(&store, OP_A, key_in(Keyspace::ROW_NUMBER_MAPPING, 1), row("durable"));
	assert!(store.flush_pending_blocking());

	assert_eq!(point_entries(&store), 1, "a cached keyspace must still be admitted after the refusal list grew");
}

#[test]
fn a_group_dictionary_key_round_trips_through_the_point_tier() {
	// GROUP_DICTIONARY is the one cached keyspace keyed on the root group, and a tier that declines it reads
	// correctly while paying a persistent lookup on every single read.
	let (store, _storage, _guard) = cached_store();
	let key = OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::GROUP_DICTIONARY, [1]).as_encoded().clone();
	put(&store, OP_A, key.clone(), row("dictionary"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

	assert_eq!(point_entries(&store), 1, "the flush write-through must admit a root-group dictionary key");

	let before = point_tier(&store).metrics();
	for _ in 0..4 {
		assert_eq!(
			body(&store.get(OP_A, &key).expect("the dictionary key stays readable")),
			"dictionary",
			"a root-group key must read back exactly what the flush made durable"
		);
	}
	let after = point_tier(&store).metrics();

	assert_eq!(
		after.fills_started, before.fills_started,
		"the flush already held the row; a fill here means every dictionary read pays sqlite again"
	);
	assert_eq!(
		after.hits,
		before.hits + 4,
		"every read must be attributed to the tier, not silently skipped past it"
	);
}

#[test]
fn a_cached_absence_survives_the_range_fill_that_materializes_over_it() {
	// A range materialize must not cost the point tier the absences its reads paid sqlite to learn.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key_in(Keyspace::ACCUMULATOR, 9),
		post: row("gone"),
	}]);
	storage.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: key_in(Keyspace::ACCUMULATOR, 9),
		pre: DurablePre::Present(ByteSize::from_bytes(row("gone").bytes().len() as u64)),
	}]);

	assert!(
		store.get(OP_A, &key_in(Keyspace::ACCUMULATOR, 9)).is_none(),
		"the key is gone but still passes the filter"
	);
	assert_eq!(point_entries(&store), 1, "the absence itself must be cached, otherwise this test proves nothing");

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(
		range_tier(&store).partitions(),
		1,
		"the scan must claim the span it covered or nothing below is tested"
	);
	assert_eq!(point_entries(&store), 1, "the materialize must leave the remembered absence exactly where it was");
	assert_eq!(range_entries(&store), 3, "and the three scanned rows must land in the range tier beside it");

	range_tier(&store).invalidate_operator(OP_A);
	assert_eq!(
		range_tier(&store).partitions(),
		0,
		"the claim must be gone, or the range tier answers the absence and the carry is untested"
	);

	let counters = point_tier(&store).metrics();
	let before = ScanCounters::sample();
	assert!(store.get(OP_A, &key_in(Keyspace::ACCUMULATOR, 9)).is_none(), "the key is still absent from sqlite");
	let scanned = before.since();
	let after = point_tier(&store).metrics();

	assert_eq!(
		after.fills_started, counters.fills_started,
		"a re-read of an absence the tier already owns must cost no persistent lookup; a second fill means \
		 the range materialize threw the absence away and every such key is bought from sqlite twice"
	);
	assert_eq!(scanned.fetched, 0, "a point read served from the tier must reach no persistent scan either");
	assert_eq!(after.hits, counters.hits + 1, "the read must be attributed as a hit, not silently as a miss");
}

#[test]
fn a_proven_span_outranks_the_absence_the_point_tier_remembers() {
	// A stale absence that outranks the proven span covering it makes the store contradict itself: the point read
	// answers gone while the range answer returns the row, with nothing anywhere to report it.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);
	for suffix in 4..=5u8 {
		storage.apply_batch(&[OperatorWrite::Insert {
			operator: OP_A,
			key: key_in(Keyspace::ACCUMULATOR, suffix),
			post: row("later"),
		}]);
		storage.apply_batch(&[OperatorWrite::Remove {
			operator: OP_A,
			key: key_in(Keyspace::ACCUMULATOR, suffix),
			pre: DurablePre::Present(ByteSize::from_bytes(row("later").bytes().len() as u64)),
		}]);
	}

	assert!(store.get(OP_A, &key_in(Keyspace::ACCUMULATOR, 4)).is_none(), "the key starts absent");
	assert!(!store.contains(OP_A, &key_in(Keyspace::ACCUMULATOR, 5)), "and so does the one contains will read");
	assert_eq!(point_entries(&store), 2, "both absences must be remembered, or the contradiction never arises");

	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key_in(Keyspace::ACCUMULATOR, 4),
		post: row("v4"),
	}]);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key_in(Keyspace::ACCUMULATOR, 5),
		post: row("v5"),
	}]);

	let primed = store.range_batch(OP_A, accumulator_range(), 64);
	assert_eq!(
		bodies(&primed),
		["v1", "v2", "v3", "v4", "v5"],
		"the scan must find the keys the point tier believes are absent"
	);
	assert_eq!(range_tier(&store).partitions(), 1, "the scan must materialize its span or nothing below is tested");

	assert_eq!(
		body(&store
			.get(OP_A, &key_in(Keyspace::ACCUMULATOR, 4))
			.expect("the proven span must answer the point read")),
		"v4",
		"an absence that outranks the span makes the store deny a row its own range answer returns"
	);
	assert!(
		store.contains(OP_A, &key_in(Keyspace::ACCUMULATOR, 5)),
		"contains must take the same arm as get, or a branch on the key goes the wrong way"
	);
	assert_eq!(
		bodies(&store.range_batch(OP_A, accumulator_range(), 64)),
		["v1", "v2", "v3", "v4", "v5"],
		"and the range answer must still agree with the point answer"
	);

	assert_eq!(
		point_tier(&store).get(OP_A, &key_in(Keyspace::ACCUMULATOR, 4)),
		Some(Some(row("v4"))),
		"the contradicted absence must be repaired in place; left standing it answers wrongly again the \
		 moment the bucket is dropped"
	);
	assert_eq!(
		point_tier(&store).get(OP_A, &key_in(Keyspace::ACCUMULATOR, 5)),
		Some(Some(row("v5"))),
		"the contains path repairs its own absences or it leaves the same contradiction behind"
	);
}

fn sliced_store(budget: ByteSize) -> (OperatorStore, SqliteOperatorStorage, SqliteTempPathGuard) {
	// A budget this small forces one drain to run many slices, so a write-back that only covers one of them shows up.
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		commit: OperatorCommitConfig {
			storage: OperatorCommitBuffer::with_budget(budget),
		},
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage.clone()))
			.flush_interval(Duration::from_hours_const(1))),
		point: Some(OperatorPointConfig::testing()),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock,
	});
	(store, storage, guard)
}



#[test]
fn a_drain_that_runs_many_slices_writes_back_every_slice_and_not_just_one() {
	// Each slice carries its own rows, so a write-back wired to fire once per drain instead of once per
	// slice leaves whichever slices it missed uncached while their rows are already durable.
	let (store, _storage, _guard) = sliced_store(ByteSize::from_bytes(64));
	for suffix in 1..=8u8 {
		put(&store, OP_A, key(suffix), row(&format!("v{suffix}")));
	}

	store.commit().flush_all();

	assert_eq!(
		point_entries(&store),
		8,
		"every slice of the drain must write its own rows back; a first-slice-only or last-slice-only \
		 write-back leaves the rest durable but uncached"
	);
}

