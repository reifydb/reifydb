// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicBool, Ordering};

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::value::duration::Duration;

use crate::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	flush::flush_now,
	sqlite::SqliteOperatorStorage,
	store::{CheckpointInterlock, StandardOperatorStore},
	tier::{persistent::OperatorPersistentTier, point::OperatorPointConfig, range::OperatorRangeConfig},
};

const FLOW_A: FlowId = FlowId(1);
const FLOW_B: FlowId = FlowId(2);

fn store_fixture() -> (StandardOperatorStore, SqliteTempPathGuard) {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let store = StandardOperatorStore::new(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage))
			.flush_interval(Duration::from_hours_const(1))),
		point: Some(OperatorPointConfig::testing()),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock,
	});
	(store, guard)
}

fn flush(store: &StandardOperatorStore) {
	flush_now(&store.commit);
}

fn flush_once_interlock() -> CheckpointInterlock {
	let fired = AtomicBool::new(false);
	Box::new(move |store| {
		if fired.swap(true, Ordering::SeqCst) {
			return;
		}
		flush(store);
	})
}

#[test]
fn a_flush_between_the_two_tier_reads_cannot_hide_a_buffered_checkpoint_delete() {
	let (store, _guard) = store_fixture();

	store.checkpoint_set(FLOW_A, CommitVersion(10));
	store.checkpoint_set(FLOW_B, CommitVersion(20));
	flush(&store);
	assert_eq!(store.checkpoint_list(), vec![FLOW_A, FLOW_B], "both checkpoints must start out durable");

	store.checkpoint_delete(FLOW_A);
	store.attach_checkpoint_interlock(flush_once_interlock());

	assert_eq!(
		store.checkpoint_list(),
		vec![FLOW_B],
		"the delete is buffered and the interlock flushes it mid-merge; reading the persistent tier first \
		 would see the pre-flush row for flow A and then find an already-drained buffer, resurrecting a \
		 checkpoint the caller deleted"
	);
}

#[test]
fn a_flush_between_the_two_tier_reads_cannot_raise_the_floor_over_a_buffered_checkpoint() {
	let (store, _guard) = store_fixture();

	store.checkpoint_set(FLOW_A, CommitVersion(100));
	flush(&store);
	assert_eq!(store.checkpoint_floor(), Some(CommitVersion(100)), "the durable checkpoint sets the floor");

	store.checkpoint_set(FLOW_B, CommitVersion(50));
	store.attach_checkpoint_interlock(flush_once_interlock());

	assert_eq!(
		store.checkpoint_floor(),
		Some(CommitVersion(50)),
		"the lower checkpoint is buffered and the interlock flushes it mid-merge; reading the persistent \
		 tier first would miss it in both reads and leave the floor at 100, over a checkpoint at 50"
	);
}

#[test]
fn a_checkpoint_merge_without_an_interlock_is_unaffected() {
	let (store, _guard) = store_fixture();

	store.checkpoint_set(FLOW_A, CommitVersion(10));
	store.checkpoint_set(FLOW_B, CommitVersion(20));
	flush(&store);
	store.checkpoint_delete(FLOW_A);

	assert_eq!(store.checkpoint_list(), vec![FLOW_B], "a buffered delete must mask the durable row");
	assert_eq!(
		store.checkpoint_floor(),
		Some(CommitVersion(10)),
		"the floor must hold at the durable version of a flow whose delete is still buffered, because a \
		 crash loses the buffer and sends that flow back to 10; raising the floor to 20 would let retention \
		 reap the versions the restart needs"
	);
}
