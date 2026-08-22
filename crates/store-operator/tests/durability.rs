// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What survives a restart, which a testscript cannot express because it cannot rebuild the store. Every store
//! here is opened over a real sqlite file with its flush interval parked an hour out, so the only thing that
//! ever becomes durable is what the test explicitly flushed, and a second store opened over the same file is
//! what a boot after a crash would see.

use std::path::Path;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{point::OperatorPointConfig, range::OperatorRangeConfig},
	types::OperatorWrite,
};
use reifydb_testing::tempdir::temp_dir;
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

const OP: OperatorId = OperatorId(1);

const OTHER: OperatorId = OperatorId(2);

const GROUP: GroupId = GroupId(1);

const SIDE: u8 = 0;

const FLOW: FlowId = FlowId(7);

fn store_at(path: &Path) -> OperatorStore {
	// the hour-long interval means the only flush a test sees is the one it asked for
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::sqlite(SqliteConfig::new(path))
			.flush_interval(Duration::from_hours_const(1))),
		point: Some(OperatorPointConfig::default()),
		range: Some(OperatorRangeConfig::default()),
		spawner,
		clock: Clock::Real,
	})
}

fn key(suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, Keyspace::ACCUMULATOR, [suffix]).as_encoded().clone()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(store: &OperatorStore, operator: OperatorId, suffix: u8) -> Option<String> {
	store.get(operator, &key(suffix))
		.map(|row| String::from_utf8(row.body().to_vec()).expect("test bodies are utf8"))
}

#[test]
fn a_write_that_was_never_flushed_is_not_there_after_a_restart() {
	// nothing promised durability, so the write must be gone rather than half there
	temp_dir(|dir| {
		let store = store_at(dir);
		store.set(OP, key(1), row("unflushed"));
		assert_eq!(body(&store, OP, 1).as_deref(), Some("unflushed"), "the live store serves its own buffer");

		let booted = store_at(dir);

		assert!(
			booted.get(OP, &key(1)).is_none(),
			"a write that never reached sqlite cannot come back from it; if it does, the write path is \
			 bypassing the commit buffer and paying a synchronous sqlite write on every flow commit"
		);
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_flushed_write_is_there_after_a_restart() {
	temp_dir(|dir| {
		let store = store_at(dir);
		store.set(OP, key(1), row("flushed"));
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
				&[OperatorWrite::Set {
					operator: OP,
					key: key(*suffix),
					row: row(&format!("slice-{suffix}")),
				}],
				&[(FLOW, CommitVersion(*version))],
				&[],
			);
			if index == 0 {
				assert!(store.flush_pending_blocking(), "the first slice must be made durable");
			}
		}

		let booted = store_at(dir);
		let checkpoint =
			booted.checkpoint_get(FLOW).expect("the first slice was flushed, so a checkpoint is durable");

		for (suffix, version) in slices.iter() {
			if *version <= checkpoint.0 {
				assert!(
					booted.get(OP, &key(*suffix)).is_some(),
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
			booted.get(OP, &key(2)).is_none(),
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
		store.checkpoint_set(FLOW, CommitVersion(10));
		assert!(store.flush_pending_blocking(), "the older checkpoint has to be durable before this is a test");

		store.checkpoint_set(FLOW, CommitVersion(80));

		assert_eq!(
			store.checkpoint_get(FLOW),
			Some(CommitVersion(80)),
			"the layered read still serves the buffered checkpoint"
		);
		assert_eq!(
			store.checkpoint_floor(),
			Some(CommitVersion(10)),
			"the floor must stay at the flushed version; advancing it lets retention reap versions 10..80, \
			 which a restart would send the flow straight back to"
		);

		let booted = store_at(dir);
		assert_eq!(
			booted.checkpoint_get(FLOW),
			Some(CommitVersion(10)),
			"the restart confirms the floor was right about what is durable"
		);

		assert!(store.flush_pending_blocking(), "the newer checkpoint must reach the flusher");
		assert_eq!(
			store.checkpoint_floor(),
			Some(CommitVersion(80)),
			"once durable the floor must advance, otherwise retention is pinned at the first checkpoint the \
			 database ever wrote and nothing is reaped again"
		);

		let rebooted = store_at(dir);
		assert_eq!(rebooted.checkpoint_get(FLOW), Some(CommitVersion(80)));
		assert_eq!(rebooted.checkpoint_floor(), Some(CommitVersion(80)));
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_drop_recorded_before_a_flush_is_still_a_drop_after_a_restart() {
	// a lost marker boots a recreated operator on the dead operator's state
	temp_dir(|dir| {
		let store = store_at(dir);
		store.set(OP, key(1), row("before"));
		store.set(OTHER, key(1), row("neighbour"));
		store.anchor_set(OP, GROUP, SIDE, RowNumber(1), DateTime::from_millis(100));
		assert!(store.flush_pending_blocking(), "the pre-drop rows must be durable for the drop to have work");

		store.drop_operator_state(OP);
		store.set(OP, key(2), row("after"));
		assert!(store.flush_pending_blocking(), "the marker and the later write travel in one batch");

		let booted = store_at(dir);

		assert!(
			booted.get(OP, &key(1)).is_none(),
			"the marker must erase the rows it only masked in memory, otherwise the drop is undone by the \
			 first restart"
		);
		assert!(
			booted.anchor_get(OP, GROUP, SIDE, RowNumber(1)).is_none(),
			"dropping operator state takes that operator's anchors with it"
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

		store.set(OP, key(1), row("once"));
		store.checkpoint_set(FLOW, CommitVersion(5));
		assert!(store.flush_pending_blocking(), "the write must reach the flusher");
		assert!(
			store.flush_pending_blocking(),
			"a repeat flush of an already drained buffer must still succeed"
		);
		assert!(store.flush_pending_blocking(), "and again");

		let booted = store_at(dir);
		assert_eq!(body(&booted, OP, 1).as_deref(), Some("once"), "repeated flushes must not lose the row");
		assert_eq!(booted.checkpoint_get(FLOW), Some(CommitVersion(5)));

		store.set(OP, key(2), row("after-the-repeats"));
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
