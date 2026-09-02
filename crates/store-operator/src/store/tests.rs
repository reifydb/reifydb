// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	sync::atomic::{AtomicBool, Ordering},
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupId, KeyspaceId, group_inner_range, keyspace_inner_range},
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_testing::keyspace::state_key;
use reifydb_value::{byte_size::ByteSize, util::hash::Hash128};

use crate::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::{CheckpointInterlock, StandardOperatorStore},
	tier::{
		persistent::{OperatorPersistentTier, sqlite::SqliteOperatorStorage},
		point::OperatorPointConfig,
		range::OperatorRangeConfig,
		resident::flush::actor::flush_now,
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
const KEYSPACE: KeyspaceId = KeyspaceId::JOIN_LEFT;

fn group() -> GroupId {
	GroupId::hashed(Hash128(7))
}

fn key(suffix: u8) -> EncodedKey {
	state_key(group(), KEYSPACE, suffix as u64)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn all() -> EncodedKeyRange {
	keyspace_inner_range(group(), KEYSPACE)
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
	store.state_last_iter(OP, all()).next().map(|(key, row)| (key.as_slice()[key.len() - 1], body(&row)))
}

const SWEEP_BUDGET: u64 = 1024;

fn sweep_group(id: u128) -> GroupId {
	GroupId::hashed(Hash128(id))
}

fn sweep_key(group: GroupId, keyspace: KeyspaceId, suffix: u64) -> EncodedKey {
	state_key(group, keyspace, suffix)
}

fn sweep_insert(group: GroupId, keyspace: KeyspaceId, suffix: u64, value: &str) -> OperatorWrite {
	OperatorWrite::Insert {
		operator: OP,
		key: sweep_key(group, keyspace, suffix),
		post: row(value),
	}
}

fn seed_groups(store: &StandardOperatorStore, groups: &[GroupId]) {
	let mut writes = Vec::new();
	for group in groups {
		for keyspace in [KeyspaceId::JOIN_LEFT, KeyspaceId::JOIN_RIGHT] {
			for suffix in [1u64, 2] {
				writes.push(sweep_insert(*group, keyspace, suffix, "seeded"));
			}
		}
	}
	store.apply_batch(&writes);
}

fn encoded_order(groups: &[GroupId]) -> Vec<GroupId> {
	let mut ordered = groups.to_vec();
	ordered.sort_by_key(|group| Reverse(*group.as_bytes()));
	ordered
}

fn per_group_pages(store: &StandardOperatorStore, groups: &[GroupId]) -> Vec<(EncodedKey, String)> {
	let mut out = Vec::new();
	for group in encoded_order(groups) {
		out.extend(
			store.range_batch(OP, group_inner_range(group), SWEEP_BUDGET)
				.items
				.into_iter()
				.map(|(key, value)| (key, body(&value))),
		);
	}
	out
}

fn group_page(store: &StandardOperatorStore, groups: &[GroupId]) -> Vec<(EncodedKey, String)> {
	store.group_page(OP, groups, SWEEP_BUDGET)
		.items
		.into_iter()
		.map(|(key, value)| (key, body(&value)))
		.collect()
}

#[test]
fn a_group_page_answers_with_exactly_the_union_of_the_per_group_range_batches() {
	// one persistent call replaces one range read per group, so a dropped or reordered row here silently shrinks what a drain reclaims
	let (store, _guard) = store_fixture();
	let groups = [sweep_group(11), sweep_group(22), sweep_group(33)];
	seed_groups(&store, &groups);
	flush(&store);

	let expected = per_group_pages(&store, &groups);

	assert_eq!(group_page(&store, &groups), expected);
	assert_eq!(expected.len(), 12, "three groups each hold two rows in two keyspaces");
}

#[test]
fn a_group_page_answers_with_rows_that_never_reached_the_persistent_tier() {
	// the resident tier is consulted per group, and skipping it hands the reaper a group it will call empty while unflushed rows still name it
	let (store, _guard) = store_fixture();
	let groups = [sweep_group(11), sweep_group(22)];
	seed_groups(&store, &groups);

	assert_eq!(group_page(&store, &groups), per_group_pages(&store, &groups));
	assert_eq!(group_page(&store, &groups).len(), 8);
}

#[test]
fn a_group_page_hides_a_buffered_tombstone_over_a_durable_row() {
	// without the buffer shadowing the persistent read the reaper sweeps a row a caller already deleted
	let (store, _guard) = store_fixture();
	let groups = [sweep_group(11), sweep_group(22)];
	seed_groups(&store, &groups);
	flush(&store);

	let doomed = sweep_key(groups[0], KeyspaceId::JOIN_LEFT, 1);
	let pre = store
		.get(OP, &doomed)
		.map_or(DurablePre::Absent, |row| DurablePre::Present(ByteSize::from_bytes(row.bytes().len() as u64)));
	store.apply_batch(&[OperatorWrite::Remove {
		operator: OP,
		key: doomed.clone(),
		pre,
	}]);

	let swept = group_page(&store, &groups);

	assert_eq!(swept, per_group_pages(&store, &groups));
	assert!(!swept.iter().any(|(key, _)| key == &doomed));
	assert_eq!(swept.len(), 7);
}

#[test]
fn a_group_page_never_answers_with_a_group_outside_the_set() {
	// the set is the only filter left once the per group ranges are gone, so a leak here reclaims a live group
	let (store, _guard) = store_fixture();
	let groups = [sweep_group(11), sweep_group(22), sweep_group(33)];
	seed_groups(&store, &groups);
	flush(&store);

	let asked = [groups[0], groups[2]];
	let swept = group_page(&store, &asked);

	assert_eq!(swept, per_group_pages(&store, &asked));
	assert_eq!(swept.len(), 8);
}

#[test]
fn a_group_page_reports_more_work_when_the_budget_cuts_the_set_short() {
	// a page that stops mid set must say so, otherwise the reaper declares a group drained while rows survive
	let (store, _guard) = store_fixture();
	let groups = [sweep_group(11), sweep_group(22)];
	seed_groups(&store, &groups);
	flush(&store);

	let batch = store.group_page(OP, &groups, 3);

	assert_eq!(batch.items.len(), 3);
	assert!(batch.has_more);
	let full = group_page(&store, &groups);
	assert_eq!(batch.items.into_iter().map(|(key, value)| (key, body(&value))).collect::<Vec<_>>(), full[..3]);
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
