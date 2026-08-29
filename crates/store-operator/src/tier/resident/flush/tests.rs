// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	time::Instant,
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::GroupId,
};
use reifydb_runtime::{
	actor::{
		context::{CancellationToken, Context},
		mailbox::ActorRef,
		system::ActorSystem,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
	shutdown::Shutdown,
	sync::waiter::WaiterHandle,
};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use super::actor::*;
use crate::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{
		persistent::{OperatorPersistentTier, sqlite::SqliteOperatorStorage},
		point::OperatorPointConfig,
		range::OperatorRangeConfig,
		resident::{FLUSH_BUDGET_BYTES, OperatorResidentState, batch::FlushBatch},
	},
	types::{BufferedState, DurablePre, OperatorWrite},
};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);
const SIDE: u8 = 0;
const FLOW: FlowId = FlowId(7);

fn operators_in(batch: &FlushBatch) -> Vec<OperatorId> {
	let mut seen: Vec<OperatorId> = batch.state.iter().map(|((operator, _), _)| operator).collect();
	seen.sort_unstable();
	seen.dedup();
	seen
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
		resident: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage.clone()))),
		point: Some(OperatorPointConfig::testing()),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn buffer_fixture() -> (OperatorResidentState, SqliteOperatorStorage, ActorRef<FlushMessage>, SqliteTempPathGuard) {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock);
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorResidentState::new();
	buffer.attach_sinks(tier(&storage), None, None);
	let actor_ref = ResidentFlushActor::spawn(&spawner, buffer.clone());
	(buffer, storage, actor_ref, guard)
}

fn key(suffix: u8) -> EncodedKey {
	let mut bytes = 7u128.to_be_bytes().to_vec();
	bytes.push(0x10);
	bytes.push(suffix);
	EncodedKey::new(bytes)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn entry_bytes(suffix: u8, body: &str) -> ByteSize {
	ByteSize::from_bytes((key(suffix).len() + row(body).bytes().len()) as u64)
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn put(store: &OperatorStore, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
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
fn a_buffered_write_becomes_durable_and_the_buffer_stops_shadowing_it() {
	let (store, storage, _guard) = store_fixture();
	put(&store, OP_A, key(1), row("written"));

	assert!(
		storage.get(OP_A, &key(1)).is_none(),
		"the write must still be memory only before the flush; a synchronous sqlite write is exactly \
		 what the resident state exists to remove from the flow commit path"
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
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("durable"),
	}]);

	erase(&store, OP_A, &key(1));
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
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("pre-drop-durable"),
	}]);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_B,
		key: key(1),
		post: row("neighbour"),
	}]);
	storage.join_expiry_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));
	put(&store, OP_A, key(2), row("pre-drop-buffered"));

	store.drop_operator_state(OP_A);
	put(&store, OP_A, key(3), row("post-drop"));

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
		storage.join_expiry_get(OP_A, GROUP_A, SIDE, RowNumber(1)).is_none(),
		"dropping operator state takes that operator's join expiries with it"
	);
	assert!(
		storage.get(OP_B, &key(1)).is_some(),
		"the drop is scoped to one operator, otherwise one seal wipes the whole store"
	);
}

#[test]
fn a_join_expiry_drop_erases_only_the_group_it_names() {
	let (store, storage, _guard) = store_fixture();
	storage.join_expiry_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));
	storage.join_expiry_set(OP_A, GROUP_B, SIDE, RowNumber(2), DateTime::from_millis(200));
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key(1),
		post: row("durable"),
	}]);

	store.join_expiries_remove_group(OP_A, GROUP_A);
	store.join_expiry_set(OP_A, GROUP_A, SIDE, RowNumber(3), DateTime::from_millis(300));

	assert!(store.flush_pending_blocking(), "the group marker must reach the flusher");

	assert!(
		storage.join_expiry_get(OP_A, GROUP_A, SIDE, RowNumber(1)).is_none(),
		"the named group's flushed join expiries must be erased"
	);
	assert_eq!(
		storage.join_expiry_get(OP_A, GROUP_A, SIDE, RowNumber(3)),
		Some(DateTime::from_millis(300)),
		"a join expiry armed after the group drop must survive it, otherwise the group is left with no \
		 timer and never expires"
	);
	assert_eq!(
		storage.join_expiry_get(OP_A, GROUP_B, SIDE, RowNumber(2)),
		Some(DateTime::from_millis(200)),
		"a sibling group keeps its join expiries; a group-wide DELETE that ignores the group disarms every \
		 timer the operator owns"
	);
	assert!(storage.get(OP_A, &key(1)).is_some(), "a join expiry drop must never touch operator state");
}

#[test]
fn join_expiries_flush_as_upserts_and_deletes_and_are_then_served_from_sqlite() {
	let (store, storage, _guard) = store_fixture();
	storage.join_expiry_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));
	storage.join_expiry_set(OP_A, GROUP_A, SIDE, RowNumber(2), DateTime::from_millis(200));

	store.join_expiry_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(900));
	store.join_expiry_remove(OP_A, GROUP_A, SIDE, RowNumber(2));
	store.join_expiry_set(OP_A, GROUP_A, SIDE, RowNumber(3), DateTime::from_millis(300));

	assert!(store.flush_pending_blocking(), "the join expiry batch must reach the flusher");

	assert_eq!(
		storage.join_expiry_get(OP_A, GROUP_A, SIDE, RowNumber(1)),
		Some(DateTime::from_millis(900)),
		"a re-armed join expiry must upsert over the flushed expiry; inserting instead of upserting either \
		 fails the primary key or leaves the timer firing on the stale deadline"
	);
	assert!(
		storage.join_expiry_get(OP_A, GROUP_A, SIDE, RowNumber(2)).is_none(),
		"a removed join expiry must flush as a DELETE, otherwise the disarmed timer re-arms itself from \
		 sqlite"
	);
	assert_eq!(
		storage.join_expiry_get(OP_A, GROUP_A, SIDE, RowNumber(3)),
		Some(DateTime::from_millis(300)),
		"a newly armed join expiry must be inserted"
	);

	assert_eq!(
		store.join_expiry_get(OP_A, GROUP_A, SIDE, RowNumber(1)),
		Some(DateTime::from_millis(900)),
		"with the buffer drained the point read is served from sqlite and must agree with what was \
		 written"
	);
	let due = store.join_expiries_due(OP_A, GROUP_A, DateTime::from_millis(1_000), 8);
	assert_eq!(
		due.iter().map(|join_expiry| join_expiry.row_number).collect::<Vec<RowNumber>>(),
		vec![RowNumber(3), RowNumber(1)],
		"the due scan now reads sqlite alone, so it must see the re-armed expiry in its new position and \
		 must not see the deleted join expiry"
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

	put(&store, OP_A, key(1), row("after-the-empty-flushes"));
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
	let buffer = OperatorResidentState::new();
	buffer.attach_sinks(tier(&storage), None, None);
	buffer.record_state_set(OP_A, key(1), row("first"), DurablePre::Absent);

	let held = buffer.flush_guard();
	let batch = buffer.take_for_flush().expect("the running flusher takes the seeded batch");
	buffer.record_state_set(OP_A, key(2), row("second"), DurablePre::Absent);

	let ran = Arc::new(AtomicBool::new(false));
	let second = {
		let buffer = buffer.clone();
		let ran = Arc::clone(&ran);
		thread::spawn(move || {
			flush_now(&buffer);
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
	let buffer = OperatorResidentState::new();
	buffer.attach_sinks(tier(&storage), None, None);
	let actor_ref = ResidentFlushActor::spawn(&spawner, buffer.clone());

	let actor = ResidentFlushActor::new(buffer.clone());
	let cancel = CancellationToken::new();
	let ctx = Context::new(actor_ref, actor_system.clone(), cancel.clone());
	let mut state = actor.init(&ctx);

	buffer.record_state_set(OP_A, key(1), row("pending-at-cancel"), DurablePre::Absent);
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
	let buffer = OperatorResidentState::new();
	buffer.attach_sinks(tier(&storage), None, None);

	buffer.record_state_set(OP_A, key(1), row("never-written"), DurablePre::Absent);
	storage.shutdown();

	flush_now(&buffer);
}

#[test]
#[should_panic(expected = "flushed before its sinks were attached")]
fn a_flush_before_the_sinks_are_attached_panics_instead_of_dropping_the_batch() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key(1), row("never-written"), DurablePre::Absent);

	flush_now(&buffer);
}

#[test]
fn a_batch_spanning_more_than_one_chunk_writes_and_removes_every_row() {
	let (store, storage, _guard) = store_fixture();
	let suffixes: Vec<u8> = (0..150).collect();

	for &suffix in &suffixes {
		put(&store, OP_A, key(suffix), row("chunked"));
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
		erase(&store, OP_A, &key(suffix));
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
	let buffer = OperatorResidentState::with_budget(entry_bytes(0, "v00") * 8);
	buffer.attach_sinks(tier(&storage), None, None);
	for index in 0..67 {
		buffer.record_state_set(OP_A, key(index), row(&format!("v{index}")), DurablePre::Absent);
	}

	flush_now(&buffer);

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
	let buffer =
		OperatorResidentState::with_budget(entry_bytes(1, "early").saturating_add(entry_bytes(2, "filler")));
	buffer.attach_sinks(tier(&storage), None, None);
	buffer.record_state_set(OP_A, key(1), row("early"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key(2), row("filler"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key(3), row("tail"), DurablePre::Absent);

	let first = buffer.take_for_flush().expect("the seeded buffer yields a first slice");
	storage.flush_batch(&first);
	buffer.complete_flush();
	assert_eq!(storage.get(OP_A, &key(1)).map(|row| body(&row)), Some("early".to_string()));

	buffer.record_state_set(
		OP_A,
		key(1),
		row("late"),
		DurablePre::Present(ByteSize::from_bytes(row("early").bytes().len() as u64)),
	);
	flush_now(&buffer);

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
	let buffer = OperatorResidentState::with_budget(entry_bytes(0, "at-shutdown") * 4);
	buffer.attach_sinks(tier(&storage), None, None);
	let actor_ref = ResidentFlushActor::spawn(&spawner, buffer.clone());

	let actor = ResidentFlushActor::new(buffer.clone());
	let ctx = Context::new(actor_ref, actor_system.clone(), CancellationToken::new());
	let mut state = actor.init(&ctx);

	for index in 0..41 {
		buffer.record_state_set(OP_A, key(index), row("at-shutdown"), DurablePre::Absent);
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
	let buffer = OperatorResidentState::with_budget(entry_bytes(0, "at-cancel") * 4);
	buffer.attach_sinks(tier(&storage), None, None);
	let actor_ref = ResidentFlushActor::spawn(&spawner, buffer.clone());

	let actor = ResidentFlushActor::new(buffer.clone());
	let cancel = CancellationToken::new();
	let ctx = Context::new(actor_ref, actor_system.clone(), cancel.clone());
	let mut state = actor.init(&ctx);

	for index in 0..37 {
		buffer.record_state_set(OP_A, key(index), row("at-cancel"), DurablePre::Absent);
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
	let entries = 16u8;
	let budget = entry_bytes(0, "under-the-budget") * (entries - 1) as u64;
	let buffer = OperatorResidentState::with_budget(budget);
	buffer.attach_sinks(tier(&storage), None, None);
	let actor_ref = ResidentFlushActor::spawn(&spawner, buffer.clone());
	buffer.attach_flusher(actor_ref);

	for index in 0..entries - 1 {
		buffer.record_state_set(OP_A, key(index), row("under-the-budget"), DurablePre::Absent);
	}
	thread::sleep(Duration::from_milliseconds_const(100).to_std());
	assert!(
		storage.get(OP_A, &key(0)).is_none(),
		"a buffer resting exactly on the byte budget must not be flushed early; the budget is the window, \
		 and a trigger that fires on it stops the buffer batching at all"
	);

	buffer.record_state_set(OP_A, key(entries - 1), row("under-the-budget"), DurablePre::Absent);

	let deadline = Instant::now() + Duration::from_seconds_const(5).to_std();
	while Instant::now() < deadline && storage.get(OP_A, &key(0)).is_none() {
		thread::sleep(Duration::from_milliseconds_const(5).to_std());
	}
	assert_eq!(
		storage.get(OP_A, &key(0)).map(|row| body(&row)),
		Some("under-the-budget".to_string()),
		"crossing the byte budget must make the coldest key durable on its own; the flush interval here \
		 is an hour, so anything less means only the timer can ever drain the buffer"
	);
	assert!(
		buffer.metrics().backlog <= buffer.budget(),
		"the pressure pass must leave the buffer at or under its cap; a pass that stops before it gets \
		 there lets the buffer grow without bound"
	);
}

#[test]
fn a_hair_over_the_cap_drains_the_whole_flow_and_its_checkpoint_in_one_batch() {
	let (storage, _guard) = SqliteOperatorStorage::in_memory();
	let large = "x".repeat(64 * 1024);
	let resident = 64u8;
	let cap = entry_bytes(0, &large) * resident as u64;
	let buffer = OperatorResidentState::with_budget(cap);
	buffer.attach_sinks(tier(&storage), None, None);

	let mut writes: Vec<OperatorWrite> = (0..resident)
		.map(|index| OperatorWrite::Insert {
			operator: OP_A,
			key: key(index),
			post: row(&large),
		})
		.collect();
	writes.push(OperatorWrite::Insert {
		operator: OP_B,
		key: key(resident),
		post: row(&large),
	});
	buffer.apply_batch_with_checkpoints(&writes, &[(FLOW, CommitVersion(41))], &[]);

	let slice = buffer.take_for_flush().expect("a buffer past its cap must yield a slice");

	assert_eq!(
		operators_in(&slice),
		vec![OP_A, OP_B],
		"both operators of the flow must leave in one batch; stopping at the byte budget between them \
		 strands the second operator's rows while the flow's checkpoint claims they are applied"
	);
	assert_eq!(
		slice.state.len(),
		resident as usize + 1,
		"one entry of overshoot must drain every row of the flow that owns it; a partial take strands \
		 rows in memory that the durable checkpoint already claims are applied"
	);
	assert!(
		slice.bytes > cap,
		"the byte budget is a hint, not a cut: keeping the flow whole must be allowed to overshoot it, \
		 otherwise the split that the checkpoint gate exists to prevent comes back"
	);
	assert_eq!(
		slice.checkpoints.get(&FLOW).copied().flatten(),
		Some(CommitVersion(41)),
		"the checkpoint must ride out in the same batch as the last of its flow's state; written earlier \
		 it replays nothing, written later a crash between the two replays state that is already durable"
	);
}

fn flusher_fixture()
-> (ResidentFlushActor, OperatorResidentState, Context<FlushMessage>, ActorSystem, SqliteTempPathGuard) {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock);
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let buffer = OperatorResidentState::new();
	buffer.attach_sinks(tier(&storage), None, None);
	let actor_ref = ResidentFlushActor::spawn(&spawner, buffer.clone());
	let actor = ResidentFlushActor::new(buffer.clone());
	let ctx = Context::new(actor_ref, actor_system.clone(), CancellationToken::new());
	(actor, buffer, ctx, actor_system, guard)
}

#[test]
fn a_flusher_with_no_config_attached_keeps_the_compiled_default_budget() {
	let (_actor, buffer, _ctx, _system, _guard) = flusher_fixture();

	assert_eq!(
		buffer.budget(),
		FLUSH_BUDGET_BYTES,
		"an unattached flusher must keep the compiled default rather than adopt zero"
	);
	assert_eq!(
		FLUSH_BUDGET_BYTES,
		ByteSize::from_kib(64),
		"the compiled fallback must be the testing budget under a test build"
	);
}
