// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::atomic::{AtomicBool, Ordering},
	thread,
	time::Instant,
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::GroupId,
};
use reifydb_runtime::{
	actor::{context::CancellationToken, system::ActorSystem},
	context::clock::Clock,
	shutdown::Shutdown,
};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use super::*;
use crate::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	sqlite::SqliteOperatorStorage,
	store::OperatorStore,
	tier::{persistent::OperatorPersistentTier, read::OperatorReadBufferConfig},
	types::BufferedState,
};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);
const SIDE: u8 = 0;
const FLOW: FlowId = FlowId(7);

fn idle_interval() -> Duration {
	Duration::from_hours_const(1)
}

fn tier(storage: &SqliteOperatorStorage) -> OperatorPersistentTier {
	OperatorPersistentTier::Sqlite(storage.clone())
}

fn store_fixture() -> (OperatorStore, SqliteOperatorStorage, SqliteTempPathGuard) {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage.clone()))
			.flush_interval(idle_interval())),
		read: Some(OperatorReadBufferConfig::default()),
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn buffer_fixture() -> (OperatorCommitBuffer, SqliteOperatorStorage, ActorRef<FlushMessage>, SqliteTempPathGuard) {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock);
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorCommitBuffer::new();
	let actor_ref = OperatorFlushActor::spawn(&spawner, buffer.clone(), tier(&storage), None, idle_interval());
	(buffer, storage, actor_ref, guard)
}

fn key(suffix: u8) -> EncodedKey {
	let mut bytes = 7u64.to_be_bytes().to_vec();
	bytes.push(0x10);
	bytes.push(suffix);
	EncodedKey::new(bytes)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

#[test]
fn a_buffered_write_becomes_durable_and_the_buffer_stops_shadowing_it() {
	let (store, storage, _guard) = store_fixture();
	store.set(OP_A, key(1), row("written"));

	assert!(
		storage.get(OP_A, &key(1)).is_none(),
		"the write must still be memory only before the flush; a synchronous sqlite write is exactly \
		 what the commit buffer exists to remove from the flow commit path"
	);

	assert!(store.flush_pending_blocking(), "a healthy sqlite tier must report the flush as completed");

	let durable = storage.get(OP_A, &key(1)).expect("the flush must have made the write durable");
	assert_eq!(
		body(&durable),
		"written",
		"a flush that does not reach sqlite loses every operator's state on restart while memory keeps \
		 serving it, so nothing looks wrong until the process dies"
	);

	let served = store.get(OP_A, &key(1)).expect("the key must still read back after the flush");
	assert_eq!(
		body(&served),
		"written",
		"once complete_flush drops the in-flight layer the read has to fall through to sqlite; if the \
		 flusher had not written it first the row would vanish under a live reader"
	);
}

#[test]
fn a_buffered_tombstone_flushes_as_a_delete_so_the_row_cannot_resurrect() {
	let (store, storage, _guard) = store_fixture();
	storage.set(OP_A, key(1), row("durable"));

	store.remove(OP_A, &key(1));
	assert!(store.flush_pending_blocking(), "the tombstone must reach the flusher");

	assert!(
		storage.get(OP_A, &key(1)).is_none(),
		"the tombstone must be executed as a DELETE, not skipped as an absent write"
	);
	assert!(
		store.get(OP_A, &key(1)).is_none(),
		"with the buffer drained the read reaches sqlite; a skipped delete resurrects the row the \
		 operator already removed"
	);
}

#[test]
fn a_drop_flushes_before_the_writes_recorded_after_it() {
	let (store, storage, _guard) = store_fixture();
	storage.set(OP_A, key(1), row("pre-drop-durable"));
	storage.set(OP_B, key(1), row("neighbour"));
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));
	store.set(OP_A, key(2), row("pre-drop-buffered"));

	store.drop_operator_state(OP_A);
	store.set(OP_A, key(3), row("post-drop"));

	assert!(store.flush_pending_blocking(), "the marker and the later write travel in one batch");

	let survivor =
		storage.get(OP_A, &key(3)).expect("the write recorded after the drop must survive the drop's DELETE");
	assert_eq!(
		body(&survivor),
		"post-drop",
		"running the writes before the markers deletes the post-drop write again, and a recreated \
		 operator silently starts from empty state"
	);
	assert!(
		storage.get(OP_A, &key(1)).is_none(),
		"the marker must erase the flushed rows the drop only masked in memory"
	);
	assert!(
		storage.get(OP_A, &key(2)).is_none(),
		"a pre-drop buffered write must never be replayed into sqlite behind the drop"
	);
	assert!(
		storage.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(1)).is_none(),
		"dropping operator state takes that operator's anchors with it"
	);
	assert!(
		storage.get(OP_B, &key(1)).is_some(),
		"the drop is scoped to one operator, otherwise one seal wipes the whole store"
	);
}

#[test]
fn an_anchor_drop_erases_only_the_group_it_names() {
	let (store, storage, _guard) = store_fixture();
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));
	storage.anchor_set(OP_A, GROUP_B, SIDE, RowNumber(2), DateTime::from_millis(200));
	storage.set(OP_A, key(1), row("durable"));

	store.anchors_remove_group(OP_A, GROUP_A);
	store.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(3), DateTime::from_millis(300));

	assert!(store.flush_pending_blocking(), "the group marker must reach the flusher");

	assert!(
		storage.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(1)).is_none(),
		"the named group's flushed anchors must be erased"
	);
	assert_eq!(
		storage.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(3)),
		Some(DateTime::from_millis(300)),
		"an anchor armed after the group drop must survive it, otherwise the group is left with no timer \
		 and never seals"
	);
	assert_eq!(
		storage.anchor_get(OP_A, GROUP_B, SIDE, RowNumber(2)),
		Some(DateTime::from_millis(200)),
		"a sibling group keeps its anchors; a group-wide DELETE that ignores the group disarms every \
		 timer the operator owns"
	);
	assert!(storage.get(OP_A, &key(1)).is_some(), "an anchor drop must never touch operator state");
}

#[test]
fn anchors_flush_as_upserts_and_deletes_and_are_then_served_from_sqlite() {
	let (store, storage, _guard) = store_fixture();
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(2), DateTime::from_millis(200));

	store.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(900));
	store.anchor_remove(OP_A, GROUP_A, SIDE, RowNumber(2));
	store.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(3), DateTime::from_millis(300));

	assert!(store.flush_pending_blocking(), "the anchor batch must reach the flusher");

	assert_eq!(
		storage.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(1)),
		Some(DateTime::from_millis(900)),
		"a re-armed anchor must upsert over the flushed expiry; inserting instead of upserting either \
		 fails the primary key or leaves the seal firing on the stale deadline"
	);
	assert!(
		storage.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(2)).is_none(),
		"a removed anchor must flush as a DELETE, otherwise the disarmed timer re-arms itself from sqlite"
	);
	assert_eq!(
		storage.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(3)),
		Some(DateTime::from_millis(300)),
		"a newly armed anchor must be inserted"
	);

	assert_eq!(
		store.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(1)),
		Some(DateTime::from_millis(900)),
		"with the buffer drained the point read is served from sqlite and must agree with what was \
		 written"
	);
	let due = store.anchors_due(OP_A, GROUP_A, DateTime::from_millis(1_000), 8);
	assert_eq!(
		due.iter().map(|anchor| anchor.row_number).collect::<Vec<RowNumber>>(),
		vec![RowNumber(3), RowNumber(1)],
		"the due scan now reads sqlite alone, so it must see the re-armed expiry in its new position and \
		 must not see the deleted anchor"
	);
}

#[test]
fn a_checkpoint_upsert_and_a_checkpoint_delete_both_reach_the_table() {
	let (buffer, storage, actor_ref, _guard) = buffer_fixture();

	buffer.record_checkpoint_set(FLOW, CommitVersion(41));
	buffer.record_checkpoint_set(FLOW, CommitVersion(42));
	assert!(flush_pending(&actor_ref), "the checkpoint batch must reach the flusher");

	assert_eq!(
		storage.checkpoint_get(FLOW),
		Some(CommitVersion(42)),
		"the flushed checkpoint must be the latest recorded version; a plain INSERT would collide with \
		 the previous row and a stale version replays slices that were already applied"
	);

	buffer.record_checkpoint_set(FLOW, CommitVersion(99));
	assert!(flush_pending(&actor_ref), "a later checkpoint must overwrite the flushed one");
	assert_eq!(
		storage.checkpoint_get(FLOW),
		Some(CommitVersion(99)),
		"the upsert must move the version forward rather than keep the first write"
	);

	buffer.record_checkpoint_delete(FLOW);
	assert!(flush_pending(&actor_ref), "the checkpoint tombstone must reach the flusher");
	assert!(
		storage.checkpoint_get(FLOW).is_none(),
		"a deleted checkpoint must leave the table, otherwise a dropped flow resumes from the version \
		 of a flow that no longer exists"
	);
}

#[test]
fn an_empty_flush_is_a_no_op_that_leaves_the_flusher_able_to_flush_again() {
	let (store, storage, _guard) = store_fixture();

	assert!(store.flush_pending_blocking(), "an empty buffer has nothing to write and must still succeed");
	assert!(store.flush_pending_blocking(), "a second empty flush must behave exactly like the first");

	store.set(OP_A, key(1), row("after-the-empty-flushes"));
	assert!(store.flush_pending_blocking(), "the flusher must still be usable after an empty drain");

	let durable = storage.get(OP_A, &key(1)).expect("the write after two empty flushes must be durable");
	assert_eq!(
		body(&durable),
		"after-the-empty-flushes",
		"an empty tick that leaves the reentrancy flag set turns every later flush into a refused drain, \
		 so the buffer grows without bound and nothing is ever persisted"
	);
}

#[test]
fn a_flush_waits_for_the_running_one_instead_of_taking_a_batch_beside_it() {
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key(1), row("first"));

	let held = buffer.flush_guard();
	let batch = buffer.take_for_flush().expect("the running flusher takes the seeded batch");
	buffer.record_state_set(OP_A, key(2), row("second"));

	let ran = Arc::new(AtomicBool::new(false));
	let second = {
		let buffer = buffer.clone();
		let storage = storage.clone();
		let ran = Arc::clone(&ran);
		thread::spawn(move || {
			flush_now(&buffer, &tier(&storage), None);
			ran.store(true, Ordering::Release);
		})
	};

	thread::sleep(Duration::from_milliseconds_const(50).to_std());
	assert!(
		!ran.load(Ordering::Acquire),
		"the second flush must wait; taking a batch beside a running flush replaces the in-flight \
		 layer, and the first batch's rows are then invisible to every reader while also not durable"
	);
	let BufferedState::Row(readable) = buffer.lookup_state(OP_A, &key(1)) else {
		panic!("the running flusher's rows stay readable until it says they are durable")
	};
	assert_eq!(body(&readable), "first", "the running flusher's rows stay readable until it says they are durable");

	storage.flush_batch(&batch);
	buffer.complete_flush();
	drop(held);

	second.join().expect("the waiting flush must finish");

	assert_eq!(
		storage.get(OP_A, &key(1)).map(|row| body(&row)),
		Some("first".to_string()),
		"the first batch reached sqlite"
	);
	assert_eq!(
		storage.get(OP_A, &key(2)).map(|row| body(&row)),
		Some("second".to_string()),
		"the waiting flush wrote what arrived while it waited; returning early instead would report \
		 a durable store to a shutdown that then closes the connection under the running flusher"
	);
}

#[test]
fn a_cancelled_flusher_answers_the_pending_flush_instead_of_eating_it() {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock);
	let spawner = actor_system.spawner();
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorCommitBuffer::new();
	let actor_ref = OperatorFlushActor::spawn(&spawner, buffer.clone(), tier(&storage), None, idle_interval());

	let actor = OperatorFlushActor::new(buffer.clone(), tier(&storage), None, idle_interval());
	let cancel = CancellationToken::new();
	let ctx = Context::new(actor_ref, actor_system.clone(), cancel.clone());
	let mut state = actor.init(&ctx);

	buffer.record_state_set(OP_A, key(1), row("pending-at-cancel"));
	cancel.cancel();

	let waiter = Arc::new(WaiterHandle::new());
	let directive = actor.handle(
		&mut state,
		FlushMessage::FlushPending {
			waiter: Arc::clone(&waiter),
		},
		&ctx,
	);

	assert!(matches!(directive, Directive::Stop), "a cancelled flusher must still stop");
	assert!(
		waiter.wait_timeout(Duration::from_milliseconds_const(1)),
		"the waiter must be notified before the actor leaves; an unanswered caller waits out the full \
		 timeout and then aborts the process on a flush that actually succeeded"
	);
	assert_eq!(
		storage.get(OP_A, &key(1)).map(|row| body(&row)),
		Some("pending-at-cancel".to_string()),
		"the cancelled path must still drain, otherwise the reply is a lie in the other direction"
	);
}

#[test]
#[should_panic(expected = "operator state flush ran without an open connection")]
fn a_flush_that_cannot_reach_sqlite_panics_instead_of_dropping_the_batch() {
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorCommitBuffer::new();

	buffer.record_state_set(OP_A, key(1), row("never-written"));
	storage.shutdown();

	flush_now(&buffer, &tier(&storage), None);
}

#[test]
fn a_batch_spanning_more_than_one_chunk_writes_and_removes_every_row() {
	let (store, storage, _guard) = store_fixture();
	let suffixes: Vec<u8> = (0..150).collect();

	for &suffix in &suffixes {
		store.set(OP_A, key(suffix), row("chunked"));
	}
	assert!(
		store.flush_pending_blocking(),
		"a set batch spanning a full 100-row chunk plus a 50-row remainder must still flush in one call"
	);
	for &suffix in &suffixes {
		let durable = storage
			.get(OP_A, &key(suffix))
			.unwrap_or_else(|| panic!("key {suffix} must be durable once the chunked insert commits"));
		assert_eq!(
			body(&durable),
			"chunked",
			"every row must land regardless of which chunk it was batched into, or a stale VM-compiled plan \
			 silently drops rows past the first chunk"
		);
	}

	for &suffix in &suffixes {
		store.remove(OP_A, &key(suffix));
	}
	assert!(
		store.flush_pending_blocking(),
		"a remove batch spanning a full chunk plus a remainder must also flush in one call"
	);
	for &suffix in &suffixes {
		assert!(
			storage.get(OP_A, &key(suffix)).is_none(),
			"every row must be deleted regardless of which chunk it was batched into"
		);
	}
}

#[test]
fn the_memory_tier_reports_a_flush_as_complete_without_a_flusher() {
	let store = OperatorStore::testing_memory();

	assert!(
		store.flush_pending_blocking(),
		"the memory tier has nothing to persist, so reporting failure would abort a shutdown that is \
		 already durable by definition"
	);
}

#[test]
fn a_buffer_far_past_the_budget_is_still_drained_completely_by_one_flush() {
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorCommitBuffer::with_budget(8);
	for index in 0..67 {
		buffer.record_state_set(OP_A, key(index), row(&format!("v{index}")));
	}

	flush_now(&buffer, &tier(&storage), None);

	for index in 0..67 {
		let durable = storage
			.get(OP_A, &key(index))
			.unwrap_or_else(|| panic!("key {index} must be durable once the bounded flush returns"));
		assert_eq!(body(&durable), format!("v{index}"), "every slice must carry its own values to sqlite");
	}
	assert!(buffer.take_for_flush().is_none(), "a completed flush must leave nothing behind for the next tick");
}

#[test]
fn a_key_rewritten_between_two_slices_ends_durable_as_the_later_value() {
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorCommitBuffer::with_budget(2);
	buffer.record_state_set(OP_A, key(1), row("early"));
	buffer.record_state_set(OP_A, key(2), row("filler"));
	buffer.record_state_set(OP_A, key(3), row("tail"));

	let first = buffer.take_for_flush().expect("the seeded buffer yields a first slice");
	storage.flush_batch(&first);
	buffer.complete_flush();
	assert_eq!(storage.get(OP_A, &key(1)).map(|row| body(&row)), Some("early".to_string()));

	buffer.record_state_set(OP_A, key(1), row("late"));
	flush_now(&buffer, &tier(&storage), None);

	assert_eq!(
		storage.get(OP_A, &key(1)).map(|row| body(&row)),
		Some("late".to_string()),
		"the rewrite must win; a split that replays the earlier value rolls the key back under a reader \
		 that already saw the newer one"
	);
	assert_eq!(storage.get(OP_A, &key(3)).map(|row| body(&row)), Some("tail".to_string()));
}

#[test]
fn a_shutdown_drains_a_buffer_far_past_the_budget_instead_of_one_slice_of_it() {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock);
	let spawner = actor_system.spawner();
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorCommitBuffer::with_budget(4);
	let actor_ref = OperatorFlushActor::spawn(&spawner, buffer.clone(), tier(&storage), None, idle_interval());

	let actor = OperatorFlushActor::new(buffer.clone(), tier(&storage), None, idle_interval());
	let ctx = Context::new(actor_ref, actor_system.clone(), CancellationToken::new());
	let mut state = actor.init(&ctx);

	for index in 0..41 {
		buffer.record_state_set(OP_A, key(index), row("at-shutdown"));
	}
	let directive = actor.handle(&mut state, FlushMessage::Shutdown, &ctx);

	assert!(matches!(directive, Directive::Stop), "the shutdown message must still stop the actor");
	for index in 0..41 {
		assert_eq!(
			storage.get(OP_A, &key(index)).map(|row| body(&row)),
			Some("at-shutdown".to_string()),
			"key {index} was committed and acknowledged, so a shutdown that leaves it in memory loses it"
		);
	}
	assert!(buffer.take_for_flush().is_none(), "the shutdown drain must empty the buffer, not bound it");
}

#[test]
fn a_cancelled_flusher_also_drains_a_buffer_far_past_the_budget() {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock);
	let spawner = actor_system.spawner();
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorCommitBuffer::with_budget(4);
	let actor_ref = OperatorFlushActor::spawn(&spawner, buffer.clone(), tier(&storage), None, idle_interval());

	let actor = OperatorFlushActor::new(buffer.clone(), tier(&storage), None, idle_interval());
	let cancel = CancellationToken::new();
	let ctx = Context::new(actor_ref, actor_system.clone(), cancel.clone());
	let mut state = actor.init(&ctx);

	for index in 0..37 {
		buffer.record_state_set(OP_A, key(index), row("at-cancel"));
	}
	cancel.cancel();
	let waiter = Arc::new(WaiterHandle::new());
	actor.handle(
		&mut state,
		FlushMessage::FlushPending {
			waiter: Arc::clone(&waiter),
		},
		&ctx,
	);

	for index in 0..37 {
		assert_eq!(
			storage.get(OP_A, &key(index)).map(|row| body(&row)),
			Some("at-cancel".to_string()),
			"key {index} must be durable before the cancelled flusher answers its waiter"
		);
	}
}

#[test]
fn a_buffer_that_reaches_the_budget_is_flushed_without_waiting_for_the_interval() {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock);
	let spawner = actor_system.spawner();
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let budget = 16;
	let buffer = OperatorCommitBuffer::with_budget(budget);
	let actor_ref = OperatorFlushActor::spawn(&spawner, buffer.clone(), tier(&storage), None, idle_interval());
	buffer.attach_flusher(actor_ref);

	for index in 0..budget - 1 {
		buffer.record_state_set(OP_A, key(index as u8), row("under-the-budget"));
	}
	thread::sleep(Duration::from_milliseconds_const(100).to_std());
	assert!(
		storage.get(OP_A, &key(0)).is_none(),
		"a buffer under the budget must not be flushed early, otherwise the buffer stops batching at all"
	);

	buffer.record_state_set(OP_A, key(budget as u8 - 1), row("under-the-budget"));

	let deadline = Instant::now() + Duration::from_seconds_const(5).to_std();
	while Instant::now() < deadline && storage.get(OP_A, &key(0)).is_none() {
		thread::sleep(Duration::from_milliseconds_const(5).to_std());
	}
	for index in 0..budget {
		assert_eq!(
			storage.get(OP_A, &key(index as u8)).map(|row| body(&row)),
			Some("under-the-budget".to_string()),
			"key {index} must be durable from the size trigger alone; the flush interval here is an \
			 hour, so anything less means only the timer can ever drain the buffer"
		);
	}
}
