// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicBool, Ordering};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::{GroupId, KeyspaceId, OperatorStateKey, keyspace_inner_range},
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::byte_size::ByteSize;

use crate::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	flush::flush_now,
	store::{CheckpointInterlock, StandardOperatorStore},
	tier::{
		persistent::{OperatorPersistentTier, sqlite::SqliteOperatorStorage},
		point::OperatorPointConfig,
		range::OperatorRangeConfig,
	},
	types::{DurablePre, OperatorWrite},
};

const FLOW_A: FlowId = FlowId(1);
const FLOW_B: FlowId = FlowId(2);

fn store_fixture() -> (StandardOperatorStore, SqliteTempPathGuard) {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let store = StandardOperatorStore::new(OperatorStoreConfig {
		resident: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage))),
		point: Some(OperatorPointConfig::testing()),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock,
	});
	(store, guard)
}

fn flush(store: &StandardOperatorStore) {
	flush_now(&store.resident);
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

const OP: OperatorId = OperatorId(1);
const GROUP: GroupId = GroupId(7);

fn key(suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, KeyspaceId::ACCUMULATOR, [suffix]).as_encoded().clone()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn all() -> EncodedKeyRange {
	keyspace_inner_range(GROUP, KeyspaceId::ACCUMULATOR)
}

fn insert(suffix: u8, value: &str) -> OperatorWrite {
	OperatorWrite::Insert {
		operator: OP,
		key: key(suffix),
		post: row(value),
	}
}

fn remove(store: &StandardOperatorStore, suffix: u8) -> OperatorWrite {
	let pre = store
		.get(OP, &key(suffix))
		.map_or(DurablePre::Absent, |row| DurablePre::Present(ByteSize::from_bytes(row.bytes().len() as u64)));
	OperatorWrite::Remove {
		operator: OP,
		key: key(suffix),
		pre,
	}
}

fn last(store: &StandardOperatorStore) -> Option<(u8, String)> {
	store.state_last(OP, all()).map(|(key, row)| (key.as_slice()[key.len() - 1], body(&row)))
}

#[test]
fn a_buffered_tombstone_hides_the_durable_row_it_deleted_from_the_last_read() {
	let (store, _guard) = store_fixture();

	store.apply_batch(&[insert(1, "low"), insert(2, "high")]);
	flush(&store);
	assert_eq!(last(&store), Some((2, "high".to_string())), "both rows must start out durable");

	store.apply_batch(&[remove(&store, 2)]);

	assert_eq!(
		last(&store),
		Some((1, "low".to_string())),
		"the buffered tombstone on the greatest key must fall through to the next durable row below it"
	);
}

#[test]
fn a_range_whose_every_durable_row_is_buffered_away_reads_as_empty_not_as_the_lowest_row() {
	let (store, _guard) = store_fixture();

	store.apply_batch(&[insert(1, "low"), insert(2, "mid"), insert(3, "high")]);
	flush(&store);

	store.apply_batch(&[remove(&store, 3), remove(&store, 2), remove(&store, 1)]);

	assert_eq!(last(&store), None, "a range holding only tombstones has no last row");
}

#[test]
fn a_buffered_row_above_the_durable_tail_wins_the_last_read() {
	let (store, _guard) = store_fixture();

	store.apply_batch(&[insert(1, "low")]);
	flush(&store);
	store.apply_batch(&[insert(2, "buffered")]);

	assert_eq!(
		last(&store),
		Some((2, "buffered".to_string())),
		"an unflushed row above the durable tail is the last row in range"
	);
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
