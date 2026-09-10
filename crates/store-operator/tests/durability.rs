// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What survives a restart, which a testscript cannot express because it cannot rebuild the store. Every store
//! here is opened over a real sqlite file with its flush interval parked an hour out, so the only thing that
//! ever becomes durable is what the test explicitly flushed, and a second store opened over the same file is
//! what a boot after a crash would see.

use std::{
	path::Path,
	thread,
	time::{Duration, Instant},
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig, ResidentConfig},
	range::OperatorRangeConfig,
	resident::Resident,
	store::OperatorStore,
	types::OperatorWrite,
};
use reifydb_testing::{keyspace::state_key, tempdir::temp_dir};
use reifydb_value::{byte_size::ByteSize, util::hash::Hash128};

const OP: OperatorId = OperatorId(1);

const OTHER: OperatorId = OperatorId(2);

const FLOW: FlowId = FlowId(7);

fn group() -> GroupId {
	GroupId::hashed(Hash128(1))
}

fn store_at(path: &Path) -> OperatorStore {
	// the hour-long interval means the only flush a test sees is the one it asked for
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	OperatorStore::standard(OperatorStoreConfig {
		resident: Default::default(),
		persistent: Some(OperatorPersistentConfig::sqlite(SqliteConfig::new(path))),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock: Clock::Real,
	})
}

fn sliced_store_at(path: &Path, budget: ByteSize) -> OperatorStore {
	// a budget this small caps the flush slice at the same size, so a drain of eight rows needs several slices
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	OperatorStore::standard(OperatorStoreConfig {
		resident: ResidentConfig {
			storage: Resident::with_budget(budget),
			..Default::default()
		},
		persistent: Some(OperatorPersistentConfig::sqlite(SqliteConfig::new(path))),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock: Clock::Real,
	})
}

fn key(suffix: u8) -> EncodedKey {
	state_key(group(), KeyspaceId::JOIN_LEFT, suffix as u64)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(store: &OperatorStore, operator: OperatorId, suffix: u8) -> Option<String> {
	store.state_get(operator, &GroupStateKey::bound_unchecked(key(suffix)))
		.unwrap()
		.map(|row| String::from_utf8(row.body().to_vec()).expect("test bodies are utf8"))
}

fn put(store: &OperatorStore, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
	// reading the pre-image back keeps the claim truthful even when an earlier write in the same test moved the key
	let write = match store.state_get(operator, &GroupStateKey::bound_unchecked(key.clone())).unwrap() {
		Some(pre) => OperatorWrite::Replace {
			operator,
			key: GroupStateKey::bound_unchecked(key),
			pre_value_bytes: ByteSize::from_bytes(pre.bytes().len() as u64),
			post: row,
		},
		None => OperatorWrite::Insert {
			operator,
			key: GroupStateKey::bound_unchecked(key),
			post: row,
		},
	};
	store.apply_batch(&[write]);
}

#[test]
fn a_write_that_was_never_flushed_is_not_there_after_a_restart() {
	// nothing promised durability, so the write must be gone rather than half there
	temp_dir(|dir| {
		let store = store_at(dir);
		put(&store, OP, key(1), row("unflushed"));
		assert_eq!(body(&store, OP, 1).as_deref(), Some("unflushed"), "the live store serves its own buffer");

		let booted = store_at(dir);

		assert!(
			booted.state_get(OP, &GroupStateKey::bound_unchecked(key(1))).unwrap().is_none(),
			"a write that never reached sqlite cannot come back from it; if it does, the write path is \
			 bypassing the resident state and paying a synchronous sqlite write on every flow commit"
		);
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_flushed_write_is_there_after_a_restart() {
	temp_dir(|dir| {
		let store = store_at(dir);
		put(&store, OP, key(1), row("flushed"));
		assert!(store.flush_pending_blocking(), "a healthy sqlite tier must report the flush as completed");

		let booted = store_at(dir);

		assert_eq!(
			body(&booted, OP, 1).as_deref(),
			Some("flushed"),
			"a flush that does not reach sqlite loses every operator's state on restart while the live store \
			 keeps serving it, so nothing looks wrong until the process dies"
		);
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_reopened_store_never_shows_a_checkpoint_ahead_of_the_state_it_was_earned_by() {
	// a crash may lose the tail of the log but must never leave the flow resuming past state it never wrote
	temp_dir(|dir| {
		let store = store_at(dir);
		let slices = [(1u8, 10u64), (2, 20), (3, 30)];
		for (index, (suffix, version)) in slices.iter().enumerate() {
			store.apply_batch_with_checkpoints(
				&[OperatorWrite::Insert {
					operator: OP,
					key: GroupStateKey::bound_unchecked(key(*suffix)),
					post: row(&format!("slice-{suffix}")),
				}],
				&[(FLOW, CommitVersion(*version))],
				&[],
			)
			.expect("each slice moves the checkpoint forward, so the batch must be accepted");
			if index == 0 {
				assert!(store.flush_pending_blocking(), "the first slice must be made durable");
			}
		}

		let booted = store_at(dir);
		let checkpoint = booted
			.checkpoint_get(FLOW)
			.unwrap()
			.expect("the first slice was flushed, so a checkpoint is durable");

		for (suffix, version) in slices.iter() {
			if *version <= checkpoint.0 {
				assert!(
					booted.state_get(OP, &GroupStateKey::bound_unchecked(key(*suffix)))
						.unwrap()
						.is_some(),
					"the checkpoint claims slice {version} was applied, so its state has to be durable \
					 too; a checkpoint ahead of the state replays nothing and silently skips it"
				);
			}
		}

		assert_eq!(
			checkpoint,
			CommitVersion(10),
			"only the flushed slice may be durable; a later checkpoint here means the unflushed slices \
			 reached sqlite without a flush"
		);
		assert!(
			booted.state_get(OP, &GroupStateKey::bound_unchecked(key(2))).unwrap().is_none(),
			"the unflushed slices must be absent, otherwise the loop above passes for the wrong reason"
		);
		Ok(())
	})
	.unwrap();
}

#[test]
fn the_retention_floor_only_ever_reflects_a_flushed_checkpoint() {
	// the floor is what cdc retention reaps against, so it must never run ahead of what a restart restores
	temp_dir(|dir| {
		let store = store_at(dir);
		store.checkpoint_set(FLOW, CommitVersion(10)).unwrap();
		assert!(store.flush_pending_blocking(), "the older checkpoint has to be durable before this is a test");

		store.checkpoint_set(FLOW, CommitVersion(80)).unwrap();

		assert_eq!(
			store.checkpoint_get(FLOW).unwrap(),
			Some(CommitVersion(80)),
			"the layered read still serves the buffered checkpoint"
		);
		assert_eq!(
			store.checkpoint_floor().unwrap(),
			Some(CommitVersion(10)),
			"the floor must stay at the flushed version; advancing it lets retention reap versions 10..80, \
			 which a restart would send the flow straight back to"
		);

		let booted = store_at(dir);
		assert_eq!(
			booted.checkpoint_get(FLOW).unwrap(),
			Some(CommitVersion(10)),
			"the restart confirms the floor was right about what is durable"
		);

		assert!(store.flush_pending_blocking(), "the newer checkpoint must reach the flusher");
		assert_eq!(
			store.checkpoint_floor().unwrap(),
			Some(CommitVersion(80)),
			"once durable the floor must advance, otherwise retention is pinned at the first checkpoint the \
			 database ever wrote and nothing is reaped again"
		);

		let rebooted = store_at(dir);
		assert_eq!(rebooted.checkpoint_get(FLOW).unwrap(), Some(CommitVersion(80)));
		assert_eq!(rebooted.checkpoint_floor().unwrap(), Some(CommitVersion(80)));
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_drop_recorded_before_a_flush_is_still_a_drop_after_a_restart() {
	// a lost marker boots a recreated operator on the dead operator's state
	temp_dir(|dir| {
		let store = store_at(dir);
		put(&store, OP, key(1), row("before"));
		put(&store, OTHER, key(1), row("neighbour"));
		assert!(store.flush_pending_blocking(), "the pre-drop rows must be durable for the drop to have work");

		store.drop_operator(OP).unwrap();
		put(&store, OP, key(2), row("after"));
		assert!(store.flush_pending_blocking(), "the marker and the later write travel in one batch");

		let booted = store_at(dir);

		assert!(
			booted.state_get(OP, &GroupStateKey::bound_unchecked(key(1))).unwrap().is_none(),
			"the marker must erase the rows it only masked in memory, otherwise the drop is undone by the \
			 first restart"
		);
		assert_eq!(
			body(&booted, OP, 2).as_deref(),
			Some("after"),
			"a write recorded after the marker must survive it; replaying the writes before the markers \
			 deletes it again and a recreated operator starts from empty state"
		);
		assert_eq!(
			body(&booted, OTHER, 1).as_deref(),
			Some("neighbour"),
			"the drop is scoped to one operator, otherwise one drop wipes the whole store"
		);
		Ok(())
	})
	.unwrap();
}

#[test]
fn flushing_twice_writes_the_same_state_once_and_leaves_the_flusher_usable() {
	// a reentrancy flag left set turns every later drain into a silent no-op and nothing is persisted again
	temp_dir(|dir| {
		let store = store_at(dir);

		assert!(store.flush_pending_blocking(), "an empty buffer has nothing to write and must still succeed");
		assert!(store.flush_pending_blocking(), "a second empty flush must behave exactly like the first");

		put(&store, OP, key(1), row("once"));
		store.checkpoint_set(FLOW, CommitVersion(5)).unwrap();
		assert!(store.flush_pending_blocking(), "the write must reach the flusher");
		assert!(
			store.flush_pending_blocking(),
			"a repeat flush of an already drained buffer must still succeed"
		);
		assert!(store.flush_pending_blocking(), "and again");

		let booted = store_at(dir);
		assert_eq!(body(&booted, OP, 1).as_deref(), Some("once"), "repeated flushes must not lose the row");
		assert_eq!(booted.checkpoint_get(FLOW).unwrap(), Some(CommitVersion(5)));

		put(&store, OP, key(2), row("after-the-repeats"));
		assert!(store.flush_pending_blocking(), "the flusher must still be usable after the repeats");

		let rebooted = store_at(dir);
		assert_eq!(
			body(&rebooted, OP, 2).as_deref(),
			Some("after-the-repeats"),
			"a flusher wedged by the repeats would leave this write buffered forever with no error anywhere"
		);
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_drain_that_runs_many_slices_persists_every_slice_and_not_just_one() {
	// Each slice carries its own rows, so a drain wired to settle once instead of once per slice leaves whichever
	// slices it missed undurable while the resident state reports them clean.
	temp_dir(|dir| {
		let store = sliced_store_at(dir, ByteSize::from_bytes(64));
		for suffix in 1..=8u8 {
			put(&store, OP, key(suffix), row(&format!("v{suffix}")));
		}

		store.resident().flush_all();

		let booted = sliced_store_at(dir, ByteSize::from_bytes(64));
		for suffix in 1..=8u8 {
			assert_eq!(
				body(&booted, OP, suffix).as_deref(),
				Some(format!("v{suffix}").as_str()),
				"every slice of the drain must reach sqlite; a first-slice-only or last-slice-only \
				 drain leaves the rest in memory only"
			);
		}
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_restart_over_populated_state_rebuilds_the_filter_without_hiding_a_durable_row() {
	// The read path skips sqlite whenever the filter says a key was never persisted, so a rebuild that misses
	// even one durable key turns that row into a silent None. Restarting over enough keys to need several scan
	// slices, across two keyspaces, is what makes a paging bug in the rebuild visible here rather than in a flow.
	temp_dir(|dir| {
		const KEYS: u64 = 5000;

		let store = store_at(dir);
		for seed in 0..KEYS {
			put(&store, OP, state_key(group(), KeyspaceId::JOIN_LEFT, seed), row("left"));
			put(&store, OP, state_key(group(), KeyspaceId::JOIN_RIGHT, seed), row("right"));
		}
		assert!(store.flush_pending_blocking(), "the test needs every key durable before the restart");
		drop(store);

		let booted = store_at(dir);
		let deadline = Instant::now() + Duration::from_secs(30);
		while Instant::now() < deadline && !booted.filter_metrics().enabled {
			thread::sleep(Duration::from_millis(10));
		}
		assert!(
			booted.filter_metrics().enabled,
			"a store booted over populated sqlite state must rebuild its filter; while it stays unarmed \
			 every absent key costs a sqlite round trip, which is the whole cost this filter removes"
		);

		for seed in 0..KEYS {
			assert!(
				booted.state_get(
					OP,
					&GroupStateKey::bound_unchecked(state_key(
						group(),
						KeyspaceId::JOIN_LEFT,
						seed
					))
				)
				.unwrap()
				.is_some(),
				"the rebuilt filter rejected durable JOIN_LEFT key {seed}; a false negative here is \
				 silent data loss, not a slow read"
			);
			assert!(
				booted.state_get(
					OP,
					&GroupStateKey::bound_unchecked(state_key(
						group(),
						KeyspaceId::JOIN_RIGHT,
						seed
					))
				)
				.unwrap()
				.is_some(),
				"the rebuilt filter rejected durable JOIN_RIGHT key {seed}; the scan must page every \
				 keyspace table, not just the first"
			);
		}

		assert!(
			booted.state_get(
				OP,
				&GroupStateKey::bound_unchecked(state_key(group(), KeyspaceId::JOIN_LEFT, KEYS + 1))
			)
			.unwrap()
			.is_none(),
			"a key nothing ever wrote must still read as absent once the filter is armed"
		);
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_group_page_after_a_restart_sees_a_keyspace_this_store_has_not_written() {
	// the occupancy mask decides which keyspaces a group page asks sqlite about, and a write seeds it with only
	// the keyspace it touched. A store that boots over durable state and writes elsewhere first must still seed
	// the mask from what sqlite holds, or the page silently skips a keyspace and a durable row reads as absent
	temp_dir(|dir| {
		{
			let store = store_at(dir);
			put(&store, OP, key(1), row("durable"));
			assert!(store.flush_pending_blocking(), "the row must reach sqlite before the restart");
		}

		let store = store_at(dir);
		put(&store, OP, state_key(group(), KeyspaceId::JOIN_RIGHT, 9), row("fresh"));

		let batch = store.group_page(OP, &[group()], 64).unwrap();
		let bodies: Vec<String> = batch
			.items
			.iter()
			.map(|(_, row)| String::from_utf8(row.body().to_vec()).expect("test bodies are utf8"))
			.collect();

		assert!(
			bodies.iter().any(|body| body == "durable"),
			"a group page must return the durable JOIN_LEFT row even though this store only wrote JOIN_RIGHT, got {bodies:?}"
		);
		Ok(())
	})
	.unwrap();
}
