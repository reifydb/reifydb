// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupId, OperatorStateKey},
};
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use crate::{
	tier::resident::{
		OperatorResidentState,
		batch::{DropMarker, JOIN_EXPIRY_ENTRY_BYTES, StateEntry},
	},
	types::{
		BufferedJoinExpiry, BufferedState, DurablePre, JOIN_EXPIRY_KEY_BYTES, JOIN_EXPIRY_VALUE_BYTES,
		OperatorStateCensus, OperatorWrite, StoredJoinRowExpiryCensus,
	},
};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const FLOW_A: FlowId = FlowId(101);
const FLOW_B: FlowId = FlowId(102);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn insert(operator: OperatorId, name: &str, value: &str) -> OperatorWrite {
	OperatorWrite::Insert {
		operator,
		key: key(name),
		post: row(value),
	}
}

fn write_in_flow(buffer: &OperatorResidentState, flow: FlowId, version: u64, writes: &[OperatorWrite]) {
	buffer.apply_batch_with_checkpoints(writes, &[(flow, CommitVersion(version))], &[]);
}

fn operators_of(batch: &crate::tier::resident::batch::FlushBatch) -> Vec<OperatorId> {
	let mut seen: Vec<OperatorId> = batch.state.iter().map(|((operator, _), _)| operator).collect();
	seen.sort_unstable();
	seen.dedup();
	seen
}

fn key(bytes: &str) -> EncodedKey {
	EncodedKey::new(bytes.as_bytes())
}

fn state_key(tail: &str) -> EncodedKey {
	let mut bytes = vec![0u8; OperatorStateKey::KEYSPACE_INNER_OFFSET as usize];
	bytes.push(0x10);
	bytes.extend_from_slice(tail.as_bytes());
	EncodedKey::new(bytes)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &Option<EncodedPodRow>) -> String {
	row_body(row.as_ref().expect("the slot must carry a row"))
}

fn entry_body(entry: &StateEntry) -> String {
	body(&entry.post)
}

fn row_body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn entry_bytes(key_body: &str, row_body: &str) -> ByteSize {
	ByteSize::from_bytes((key(key_body).len() + row(row_body).bytes().len()) as u64)
}

fn live_bytes(buffer: &OperatorResidentState) -> ByteSize {
	let mut total = ByteSize::ZERO;
	for operator in buffer.shared().operators() {
		let Some(slot) = buffer.shared().slot(operator) else {
			continue;
		};
		let bytes = slot.inner.lock().live.bytes;
		total = total.saturating_add(bytes);
	}
	total
}

fn resident_bytes(buffer: &OperatorResidentState) -> ByteSize {
	buffer.resident_bytes()
}

fn live_bytes_of(buffer: &OperatorResidentState, operator: OperatorId) -> ByteSize {
	buffer.shared().slot(operator).map_or(ByteSize::ZERO, |slot| {
		let bytes = slot.inner.lock().live.bytes;
		bytes
	})
}

fn flushing(buffer: &OperatorResidentState) -> bool {
	buffer.shared().global.lock().flushing
}

fn any_in_flight(buffer: &OperatorResidentState) -> bool {
	buffer.shared().operators().into_iter().any(|operator| {
		buffer.shared().slot(operator).is_some_and(|slot| slot.inner.lock().in_flight.is_some())
	})
}

#[test]
fn a_removed_key_reads_back_as_a_tombstone_not_as_absent() {
	let buffer = OperatorResidentState::new();

	assert_eq!(
		buffer.lookup_state(OP_A, &key("k")),
		BufferedState::Absent,
		"a key no layer has seen must report as unknown so the read continues to sqlite"
	);

	buffer.record_state_set(OP_A, key("k"), row("v"), DurablePre::Absent);
	let BufferedState::Row(found) = buffer.lookup_state(OP_A, &key("k")) else {
		panic!("the live layer knows the key it just wrote")
	};
	assert_eq!(row_body(&found), "v", "the buffer must hand back the row that was written, not a stale one");

	buffer.record_state_remove(OP_A, key("k"), DurablePre::Absent);
	assert_eq!(
		buffer.lookup_state(OP_A, &key("k")),
		BufferedState::Tombstone,
		"a removed key must read as a tombstone; reporting it unknown would send the read to sqlite \
		 and resurrect the deleted row"
	);
}

#[test]
fn checkpoints_distinguish_a_delete_from_a_never_written_flow() {
	let buffer = OperatorResidentState::new();

	assert!(
		buffer.lookup_checkpoint(FlowId(7)).is_none(),
		"an unwritten flow must be unknown so the read falls back to the durable checkpoint"
	);

	buffer.record_checkpoint_set(FlowId(7), CommitVersion(42));
	assert_eq!(
		buffer.lookup_checkpoint(FlowId(7)),
		Some(Some(CommitVersion(42))),
		"the buffered checkpoint must win over anything still in sqlite"
	);

	buffer.record_checkpoint_delete(FlowId(7));
	assert_eq!(
		buffer.lookup_checkpoint(FlowId(7)),
		Some(None),
		"a deleted checkpoint must be a definitive miss, otherwise a dropped flow resumes from a \
		 stale version"
	);
}

#[test]
fn taken_entries_stay_readable_until_the_flush_completes() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k"), row("v"), DurablePre::Absent);
	buffer.record_state_remove(OP_A, key("gone"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(500));
	buffer.record_checkpoint_set(FlowId(3), CommitVersion(9));

	let batch = buffer.take_for_flush().expect("a non-empty live batch must be handed to the flusher");
	assert_eq!(batch.state.len(), 2, "both the write and the tombstone belong to the taken batch");

	let BufferedState::Row(found) = buffer.lookup_state(OP_A, &key("k")) else {
		panic!("the taken row must stay readable")
	};
	assert_eq!(
		row_body(&found),
		"v",
		"dropping the taken row would let a concurrent read fall through to sqlite and observe the \
		 pre-flush value"
	);
	assert_eq!(
		buffer.lookup_state(OP_A, &key("gone")),
		BufferedState::Tombstone,
		"a taken tombstone must stay a tombstone until the delete is durable"
	);
	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 0, RowNumber(1)),
		BufferedJoinExpiry::Expiry(500),
		"join expiries ride the same in-flight layer as state"
	);
	assert_eq!(
		buffer.lookup_checkpoint(FlowId(3)),
		Some(Some(CommitVersion(9))),
		"the checkpoint is only durable once its transaction commits"
	);

	buffer.complete_flush();

	assert_eq!(
		buffer.lookup_state(OP_A, &key("k")),
		BufferedState::Absent,
		"once flushed the row lives in sqlite, so the buffer must stop answering for it"
	);
	assert_eq!(
		buffer.lookup_state(OP_A, &key("gone")),
		BufferedState::Absent,
		"a flushed tombstone must stop shadowing sqlite, otherwise the key is hidden forever"
	);
	assert_eq!(buffer.lookup_join_expiry(OP_A, GROUP_A, 0, RowNumber(1)), BufferedJoinExpiry::Absent);
	assert!(buffer.lookup_checkpoint(FlowId(3)).is_none());
}

#[test]
fn a_live_write_shadows_the_same_key_in_the_in_flight_batch() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k"), row("old"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("doomed"), row("old"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_state_set(OP_A, key("k"), row("new"), DurablePre::Absent);
	buffer.record_state_remove(OP_A, key("doomed"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(900));

	let BufferedState::Row(found) = buffer.lookup_state(OP_A, &key("k")) else {
		panic!("the live write must be visible")
	};
	assert_eq!(row_body(&found), "new", "the live layer must win over the older in-flight value");
	assert_eq!(
		buffer.lookup_state(OP_A, &key("doomed")),
		BufferedState::Tombstone,
		"a live tombstone must hide the in-flight value, otherwise a delete is silently undone"
	);
	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 0, RowNumber(1)),
		BufferedJoinExpiry::Expiry(900),
		"the newer expiry must win, otherwise the seal fires against a superseded deadline"
	);
}

#[test]
fn state_range_is_ordered_operator_scoped_and_overlays_the_in_flight_batch() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("a"), row("flushing-a"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("b"), row("flushing-b"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("c"), row("flushing-c"), DurablePre::Absent);
	buffer.record_state_set(OP_B, key("b"), row("other-operator"), DurablePre::Absent);
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_state_set(OP_A, key("b"), row("live-b"), DurablePre::Absent);
	buffer.record_state_remove(OP_A, key("c"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("d"), row("live-d"), DurablePre::Absent);

	let all = buffer.state_range(OP_A, Bound::Unbounded, Bound::Unbounded).items;
	let keys: Vec<Vec<u8>> = all.iter().map(|(k, _)| k.to_vec()).collect();
	assert_eq!(
		keys,
		vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
		"the merge cursor advances in key order, so an unordered range drops or duplicates keys"
	);

	assert_eq!(body(&all[0].1), "flushing-a", "an in-flight-only key must still be reported");
	assert_eq!(body(&all[1].1), "live-b", "the live write must overlay the in-flight value for that key");
	assert!(
		all[2].1.is_none(),
		"tombstones must survive the range, otherwise the sqlite side of the merge emits a deleted row"
	);
	assert_eq!(body(&all[3].1), "live-d");

	let other = buffer.state_range(OP_B, Bound::Unbounded, Bound::Unbounded).items;
	assert_eq!(other.len(), 1, "a range must stay inside its operator, otherwise operators read each other");
	assert_eq!(body(&other[0].1), "other-operator");

	let window = buffer.state_range(OP_A, Bound::Included(&key("b")), Bound::Excluded(&key("d"))).items;
	let window_keys: Vec<Vec<u8>> = window.iter().map(|(k, _)| k.to_vec()).collect();
	assert_eq!(
		window_keys,
		vec![b"b".to_vec(), b"c".to_vec()],
		"both layers must honour the bounds, otherwise the page over-reads past its end"
	);

	let resumed = buffer.state_range(OP_A, Bound::Excluded(&key("a")), Bound::Included(&key("b"))).items;
	let resumed_keys: Vec<Vec<u8>> = resumed.iter().map(|(k, _)| k.to_vec()).collect();
	assert_eq!(
		resumed_keys,
		vec![b"b".to_vec()],
		"an excluded lower bound is how the cursor resumes a page, so it must skip the seen key"
	);
}

#[test]
fn a_window_that_spans_nothing_reads_empty_instead_of_panicking() {
	let buffer = seeded_two_layer_buffer();

	for (label, start, end) in [
		("both bounds excluded on the same key", Bound::Excluded(key("b")), Bound::Excluded(key("b"))),
		("an excluded end on the included start", Bound::Included(key("b")), Bound::Excluded(key("b"))),
		("an excluded start under the included end", Bound::Excluded(key("b")), Bound::Included(key("b"))),
		("a start past its end", Bound::Included(key("d")), Bound::Included(key("b"))),
	] {
		let window = buffer.state_range(OP_A, start.as_ref(), end.as_ref()).items;
		assert!(window.is_empty(), "{label} spans no key, so the range must report no rows");
	}
}

#[test]
fn a_window_closed_on_one_key_still_returns_that_key() {
	let buffer = seeded_two_layer_buffer();

	let window = buffer.state_range(OP_A, Bound::Included(&key("b")), Bound::Included(&key("b"))).items;
	let keys: Vec<Vec<u8>> = window.iter().map(|(k, _)| k.to_vec()).collect();
	assert_eq!(keys, vec![b"b".to_vec()], "an inclusive pair on one key must still read that key");
	assert_eq!(body(&window[0].1), "live-b", "the overlay must still apply inside a one-key window");
}

#[test]
fn a_window_that_spans_nothing_still_reports_a_pending_drop() {
	let buffer = seeded_two_layer_buffer_with_dropped_operator();

	let window = buffer.state_range(OP_A, Bound::Excluded(&key("b")), Bound::Excluded(&key("b")));
	assert!(window.items.is_empty(), "an empty span carries no rows");
	assert!(window.dropped, "a pending drop must be reported even when the span is empty");
}

fn seeded_two_layer_buffer() -> OperatorResidentState {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("a"), row("flushing-a"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("b"), row("flushing-b"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("c"), row("flushing-c"), DurablePre::Absent);
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_state_set(OP_A, key("b"), row("live-b"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("d"), row("live-d"), DurablePre::Absent);
	buffer
}

fn seeded_two_layer_buffer_with_dropped_operator() -> OperatorResidentState {
	let buffer = OperatorResidentState::new();
	buffer.record_drop(DropMarker::OperatorState(OP_A));
	buffer.record_state_set(OP_A, key("a"), row("flushing-a"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("b"), row("flushing-b"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("c"), row("flushing-c"), DurablePre::Absent);
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_state_set(OP_A, key("b"), row("live-b"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("d"), row("live-d"), DurablePre::Absent);
	buffer
}

#[test]
fn join_expiries_for_group_overlays_the_in_flight_batch_and_keeps_tombstones() {
	let buffer = OperatorResidentState::new();
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(2), DateTime::from_millis(200));
	buffer.record_join_expiry_set(OP_A, GROUP_B, 0, RowNumber(3), DateTime::from_millis(300));
	buffer.record_join_expiry_set(OP_B, GROUP_A, 0, RowNumber(4), DateTime::from_millis(400));
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(2), DateTime::from_millis(250));
	buffer.record_join_expiry_remove(OP_A, GROUP_A, 1, RowNumber(5));

	let join_expiries = buffer.join_expiries_for_group(OP_A, GROUP_A).join_expiries;
	assert_eq!(
		join_expiries,
		vec![((0u8, RowNumber(1)), Some(100)), ((0u8, RowNumber(2)), Some(250)), ((1u8, RowNumber(5)), None),],
		"the scan must stay inside one operator and group, overlay the live expiry, and keep the \
		 tombstone so the sqlite merge never re-arms a removed join expiry"
	);
}

#[test]
fn a_drop_clears_what_came_before_it_and_keeps_what_came_after() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("before"), row("v"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_state_set(OP_B, key("untouched"), row("v"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_B, GROUP_A, 0, RowNumber(2), DateTime::from_millis(200));

	buffer.record_drop(DropMarker::OperatorState(OP_A));

	buffer.record_state_set(OP_A, key("after"), row("v"), DurablePre::Absent);

	assert_eq!(
		buffer.lookup_state(OP_A, &key("before")),
		BufferedState::Dropped,
		"a write the drop erased must never be replayed into sqlite behind the drop"
	);
	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 0, RowNumber(1)),
		BufferedJoinExpiry::Dropped,
		"dropping operator state takes that operator's join expiries with it"
	);
	assert!(
		matches!(buffer.lookup_state(OP_A, &key("after")), BufferedState::Row(_)),
		"a write recorded after the drop must survive it, otherwise a recreated operator loses state"
	);
	assert!(
		matches!(buffer.lookup_state(OP_B, &key("untouched")), BufferedState::Row(_)),
		"the drop is scoped to one operator"
	);
	assert!(matches!(buffer.lookup_join_expiry(OP_B, GROUP_A, 0, RowNumber(2)), BufferedJoinExpiry::Expiry(_)));

	let batch = buffer.take_for_flush().expect("the batch carries the marker and the later write");
	assert_eq!(
		batch.drops,
		vec![DropMarker::OperatorState(OP_A)],
		"the marker must reach the flusher; clearing memory alone leaves the sqlite rows behind"
	);
	assert_eq!(batch.state.len(), 2, "only the post-drop write and the other operator's write remain");
}

#[test]
fn a_join_expiry_drop_clears_only_the_join_expiries_it_names() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k"), row("v"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_join_expiry_set(OP_A, GROUP_B, 0, RowNumber(2), DateTime::from_millis(200));

	buffer.record_drop(DropMarker::JoinExpiriesGroup(OP_A, GROUP_A));

	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 0, RowNumber(1)),
		BufferedJoinExpiry::Dropped,
		"the named group's join expiries must be gone"
	);
	assert!(
		matches!(buffer.lookup_join_expiry(OP_A, GROUP_B, 0, RowNumber(2)), BufferedJoinExpiry::Expiry(_)),
		"a sibling group keeps its join expiries, otherwise one group's drop wipes another's timers"
	);
	assert!(
		matches!(buffer.lookup_state(OP_A, &key("k")), BufferedState::Row(_)),
		"a join expiry drop must never touch operator state"
	);

	buffer.record_drop(DropMarker::JoinExpiriesOperator(OP_A));

	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_B, 0, RowNumber(2)),
		BufferedJoinExpiry::Dropped,
		"an operator-wide join expiry drop covers every group"
	);
	assert!(
		matches!(buffer.lookup_state(OP_A, &key("k")), BufferedState::Row(_)),
		"an operator-wide join expiry drop still leaves the state alone"
	);
}

#[test]
fn take_for_flush_on_an_empty_buffer_returns_none_and_leaves_flushing_clear() {
	let buffer = OperatorResidentState::new();

	assert!(buffer.take_for_flush().is_none(), "an empty tick must not open a transaction");

	assert!(
		!flushing(&buffer),
		"a refused take must leave the flag clear, otherwise every later drop blocks forever on a \
		 flush that never runs"
	);
	assert!(!any_in_flight(&buffer), "nothing was taken, so there is no in-flight layer to read through");
}

#[test]
fn a_buffer_holding_only_a_drop_is_still_worth_flushing() {
	let buffer = OperatorResidentState::new();
	buffer.record_drop(DropMarker::OperatorState(OP_A));

	let batch = buffer
		.take_for_flush()
		.expect("a drop with no writes must still flush; the rows it erases live in sqlite");
	assert_eq!(batch.drops, vec![DropMarker::OperatorState(OP_A)]);
}

#[test]
fn take_for_flush_sets_flushing_and_complete_flush_clears_it() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k"), row("v"), DurablePre::Absent);
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	assert!(flushing(&buffer), "a taken batch must mark the buffer flushing so drops wait it out");
	assert!(any_in_flight(&buffer), "the taken batch stays readable while the flush runs");

	buffer.complete_flush();

	assert!(!flushing(&buffer), "a completed flush must release waiting drops");
	assert!(!any_in_flight(&buffer), "the flushed batch now lives in sqlite and must not be read twice");
}

#[test]
fn a_drop_waits_out_an_in_flight_flush_before_clearing() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k"), row("v"), DurablePre::Absent);
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	let dropped = Arc::new(AtomicBool::new(false));
	let dropper = {
		let buffer = buffer.clone();
		let dropped = Arc::clone(&dropped);
		thread::spawn(move || {
			buffer.record_drop(DropMarker::OperatorState(OP_A));
			dropped.store(true, Ordering::Release);
		})
	};

	thread::sleep(Duration::from_milliseconds_const(50).to_std());
	assert!(
		!dropped.load(Ordering::Acquire),
		"the drop must block while a flush is in flight, otherwise it clears state the flusher is \
		 still writing"
	);
	assert!(
		matches!(buffer.lookup_state(OP_A, &key("k")), BufferedState::Row(_)),
		"the in-flight row stays readable while the drop waits"
	);

	buffer.complete_flush();

	for _ in 0..200 {
		if dropped.load(Ordering::Acquire) {
			break;
		}
		thread::sleep(Duration::from_milliseconds_const(10).to_std());
	}
	assert!(
		dropped.load(Ordering::Acquire),
		"complete_flush must wake the waiting drop; a missed notify wedges every later drop"
	);

	dropper.join().expect("the dropping thread must finish");
	assert_eq!(
		buffer.lookup_state(OP_A, &key("k")),
		BufferedState::Dropped,
		"once the flush is done the drop clears the operator and the marker deletes the flushed rows"
	);
}

#[test]
fn apply_batch_maps_every_write_variant_onto_its_entry() {
	let buffer = OperatorResidentState::new();
	buffer.apply_batch(&[
		OperatorWrite::Insert {
			operator: OP_A,
			key: key("set"),
			post: row("v"),
		},
		OperatorWrite::Remove {
			operator: OP_A,
			key: key("removed"),
			pre: DurablePre::Absent,
		},
		OperatorWrite::JoinExpiryInsert {
			operator: OP_A,
			group: GROUP_A,
			side: 1,
			row_num: RowNumber(7),
			at: DateTime::from_millis(1_234),
		},
		OperatorWrite::JoinExpiryRemove {
			operator: OP_A,
			group: GROUP_A,
			side: 1,
			row_num: RowNumber(8),
			pre: DurablePre::Present(JOIN_EXPIRY_ENTRY_BYTES),
		},
	]);

	let BufferedState::Row(set) = buffer.lookup_state(OP_A, &key("set")) else {
		panic!("a Set must land in the state map")
	};
	assert_eq!(row_body(&set), "v");
	assert_eq!(
		buffer.lookup_state(OP_A, &key("removed")),
		BufferedState::Tombstone,
		"a Remove must land as a tombstone, not as a missing entry"
	);
	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 1, RowNumber(7)),
		BufferedJoinExpiry::Expiry(1_234),
		"a join expiry set is stored as millis, matching the memory tier and the sqlite column"
	);
	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 1, RowNumber(8)),
		BufferedJoinExpiry::Tombstone,
		"a JoinExpiryRemove must tombstone the slot so the sqlite join expiry is not read back as live"
	);
}

#[test]
fn a_combined_apply_lands_the_state_and_the_checkpoints_in_one_taken_batch() {
	let buffer = OperatorResidentState::new();

	buffer.apply_batch_with_checkpoints(
		&[
			OperatorWrite::Insert {
				operator: OP_A,
				key: key("state"),
				post: row("v"),
			},
			OperatorWrite::JoinExpiryInsert {
				operator: OP_A,
				group: GROUP_A,
				side: 0,
				row_num: RowNumber(1),
				at: DateTime::from_millis(700),
			},
		],
		&[(FlowId(3), CommitVersion(12))],
		&[FlowId(4)],
	);

	let batch = buffer.take_for_flush().expect("the combined apply must dirty the buffer");

	assert_eq!(
		entry_body(batch.state.get(&(OP_A, key("state"))).expect("the state write must be in the batch")),
		"v",
		"the state of the committed slice must ride the same batch as its checkpoint"
	);
	assert_eq!(
		batch.join_expiries.get(&(OP_A, GROUP_A, 0, RowNumber(1))).copied(),
		Some(Some(700)),
		"join expiries are part of the same slice, so they must not be split from the checkpoint either"
	);
	assert_eq!(
		batch.checkpoints.get(&FlowId(3)).copied(),
		Some(Some(CommitVersion(12))),
		"the checkpoint must be in the very batch that carries the state; landing it in a later one \
		 lets a crash leave the checkpoint ahead of the state, and the flow resumes past state it \
		 never wrote"
	);
	assert_eq!(
		batch.checkpoints.get(&FlowId(4)).copied(),
		Some(None),
		"a checkpoint delete travels with the same slice, otherwise a dropped flow keeps a durable \
		 version after its state is gone"
	);

	assert!(
		buffer.take_for_flush().is_none(),
		"nothing may be left behind for a second batch; a follow-up batch is exactly the split this \
		 entry point exists to prevent"
	);
}

#[test]
fn a_combined_apply_with_nothing_to_record_leaves_the_buffer_untouched() {
	let buffer = OperatorResidentState::new();

	buffer.apply_batch_with_checkpoints(&[], &[], &[]);

	assert!(
		buffer.take_for_flush().is_none(),
		"a slice that changed nothing must not dirty the buffer, otherwise every idle commit opens a \
		 sqlite transaction"
	);
}

#[test]
fn an_empty_write_batch_leaves_the_buffer_untouched() {
	let buffer = OperatorResidentState::new();
	buffer.apply_batch(&[]);

	assert!(
		buffer.take_for_flush().is_none(),
		"an empty apply must not dirty the buffer, otherwise every idle tick opens a transaction"
	);
}

#[test]
fn a_flow_whose_operators_still_hold_live_state_does_not_get_its_checkpoint_written() {
	// the checkpoint is the resume point; writing it while state is still in ram promises sqlite rows
	// that a crash would take with it, and replay never revisits a version at or below it
	let buffer = OperatorResidentState::with_budget(entry_bytes("k1", "v1"));
	write_in_flow(&buffer, FLOW_A, 10, &[insert(OP_A, "k1", "v1")]);
	write_in_flow(&buffer, FLOW_B, 20, &[insert(OP_B, "k2", "v2")]);

	let first = buffer.take_for_flush().expect("the seeded buffer yields a slice");
	assert_eq!(
		first.checkpoints.get(&FLOW_A).copied(),
		Some(Some(CommitVersion(10))),
		"the flow this slice drained completely is safe to check point in the same transaction"
	);
	assert!(
		!first.checkpoints.contains_key(&FLOW_B),
		"the flow the budget stopped before still holds live state, so its checkpoint must wait"
	);
	buffer.complete_flush();

	let second = buffer.take_for_flush().expect("the second flow makes a second slice");
	assert_eq!(
		second.checkpoints.get(&FLOW_B).copied(),
		Some(Some(CommitVersion(20))),
		"the held checkpoint must go out with the state that earns it, or the flow never advances"
	);
}

#[test]
fn a_checkpoint_with_no_pending_state_is_written_without_waiting_for_a_drain() {
	// a flow that only advanced its cursor writes a checkpoint and no state; gating it on a drain that
	// never comes strands it and pins cdc retention forever
	let buffer = OperatorResidentState::new();
	buffer.record_checkpoint_set(FLOW_A, CommitVersion(77));

	let batch = buffer.take_for_flush().expect("a checkpoint alone is worth a transaction");
	assert!(batch.state.is_empty(), "the flow wrote no state, so none may be invented for it");
	assert_eq!(
		batch.checkpoints.get(&FLOW_A).copied(),
		Some(Some(CommitVersion(77))),
		"nothing is pending for this flow, so everything it claims is already durable"
	);
}

#[test]
fn the_flow_waiting_longest_drains_first() {
	// draining in flow-id order lets a busy low-numbered flow starve the rest; ordering by how long a
	// flow has waited is what makes every checkpoint eventually advance
	let buffer = OperatorResidentState::with_budget(entry_bytes("k1", "v1"));
	write_in_flow(&buffer, FLOW_B, 20, &[insert(OP_B, "k2", "v2")]);
	write_in_flow(&buffer, FLOW_A, 10, &[insert(OP_A, "k1", "v1")]);

	let first = buffer.take_for_flush().expect("the seeded buffer yields a slice");
	assert_eq!(
		operators_of(&first),
		vec![OP_B],
		"the flow that has been pending longest must go first even though its id sorts last"
	);
}

#[test]
fn an_operator_with_no_flow_drains_without_blocking_any_checkpoint() {
	// state that never arrived through a flow slice has no checkpoint to earn and no flow to hold back;
	// treating it as belonging to one would stall an unrelated flow behind it forever
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("orphan"), row("v"), DurablePre::Absent);
	buffer.record_checkpoint_set(FLOW_A, CommitVersion(5));

	let batch = buffer.take_for_flush().expect("the seeded buffer yields a slice");
	assert!(batch.state.contains_key(&(OP_A, key("orphan"))), "untagged state must still be made durable");
	assert_eq!(
		batch.checkpoints.get(&FLOW_A).copied(),
		Some(Some(CommitVersion(5))),
		"an operator with no flow claims no flow, so it must not hold one back"
	);
}

#[test]
fn a_buffer_far_past_the_budget_drains_whole_flows_and_loses_nothing() {
	// the budget stops the drain before it starts the next flow, never inside one; an operator split
	// across two slices would write half its state under a checkpoint that claims all of it
	let entry = entry_bytes("k0", "v0");
	let buffer = OperatorResidentState::with_budget(entry * 2);
	for index in 0..5u64 {
		write_in_flow(&buffer, FLOW_A, index + 1, &[insert(OP_A, &format!("k{index}"), &format!("v{index}"))]);
	}
	for index in 5..10u64 {
		write_in_flow(&buffer, FLOW_B, index + 1, &[insert(OP_B, &format!("k{index}"), &format!("v{index}"))]);
	}

	let mut seen: Vec<String> = Vec::new();
	let mut slices = 0;
	while let Some(batch) = buffer.take_for_flush() {
		slices += 1;
		assert!(slices <= 4, "the drain must make progress on every take or it never terminates");
		assert_eq!(
			operators_of(&batch).len(),
			1,
			"a slice that mixes operators from two flows has crossed a boundary the byte budget was \
			 only ever allowed to stop at"
		);
		for ((operator, taken), _) in &batch.state {
			assert_eq!(
				live_bytes_of(&buffer, operator),
				ByteSize::ZERO,
				"the operator kept live state behind, so this slice split it; its flow checkpoint \
				 would then promise state that never reached sqlite"
			);
			seen.push(String::from_utf8(taken.as_slice().to_vec()).expect("test keys are utf8"));
		}
		buffer.complete_flush();
	}

	assert_eq!(slices, 2, "two flows drain as two whole slices, one each");
	let mut unique = seen.clone();
	unique.sort();
	unique.dedup();
	assert_eq!(
		unique.len(),
		seen.len(),
		"a key handed out twice is a key written twice, which resurrects the value the later slice replaced"
	);
	assert_eq!(
		seen.len(),
		10,
		"every buffered key must reach exactly one slice; a key left behind by the last take is committed \
		 operator state that never becomes durable"
	);
}

#[test]
fn a_key_rewritten_during_its_flush_flushes_as_the_later_value() {
	// carrying the in-flight value into the next slice would overwrite the rewrite in sqlite and roll
	// the key silently back to a value the operator has already replaced
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k1"), row("early"), DurablePre::Absent);

	let first = buffer.take_for_flush().expect("the seeded buffer yields a first slice");
	assert_eq!(first.state.get(&(OP_A, key("k1"))).map(entry_body), Some("early".to_string()));

	buffer.record_state_set(OP_A, key("k1"), row("late"), DurablePre::Absent);
	let BufferedState::Row(found) = buffer.lookup_state(OP_A, &key("k1")) else {
		panic!("the rewritten key must read from the live layer")
	};
	assert_eq!(
		row_body(&found),
		"late",
		"the in-flight slice still holds the earlier value, so a read that prefers it serves state the \
		 operator has already replaced"
	);
	buffer.complete_flush();

	let second = buffer.take_for_flush().expect("the rewrite recorded during the flush makes a second slice");
	assert_eq!(
		second.state.get(&(OP_A, key("k1"))).map(entry_body),
		Some("late".to_string()),
		"the later slice must carry the later value; carrying the earlier one would overwrite the rewrite \
		 in sqlite and silently roll the key back"
	);
	buffer.complete_flush();
	assert!(buffer.take_for_flush().is_none(), "the drain must terminate once everything has been handed out");
}

#[test]
fn a_split_slice_carries_every_drop_marker_ahead_of_the_writes_left_behind() {
	// a marker replayed in a later slice deletes the post-drop rows an earlier slice already made
	// durable, so every marker must ride the first slice out and never appear again
	let entry = entry_bytes("k2", "post-drop");
	let buffer = OperatorResidentState::with_budget(entry);
	write_in_flow(&buffer, FLOW_A, 1, &[insert(OP_A, "k1", "pre-drop")]);
	buffer.record_drop(DropMarker::OperatorState(OP_A));
	write_in_flow(&buffer, FLOW_A, 2, &[insert(OP_A, "k2", "post-drop")]);
	write_in_flow(&buffer, FLOW_B, 3, &[insert(OP_B, "k3", "other-flow")]);

	let first = buffer.take_for_flush().expect("the seeded buffer yields a first slice");
	assert_eq!(
		first.drops,
		vec![DropMarker::OperatorState(OP_A)],
		"every marker must ride the first slice, otherwise the drop lands after writes it must precede"
	);
	assert!(
		!first.state.contains_key(&(OP_A, key("k1"))),
		"a write recorded before the drop must have been cleared, not carried into a slice"
	);
	assert!(first.state.contains_key(&(OP_A, key("k2"))), "the post-drop write rides out with its own flow");
	buffer.complete_flush();

	let second = buffer.take_for_flush().expect("the second flow makes a second slice");
	assert!(
		second.drops.is_empty(),
		"a marker replayed in a later slice deletes the post-drop rows the first slice already made durable"
	);
	assert!(
		second.state.contains_key(&(OP_B, key("k3"))),
		"the flow the budget stopped before must be the one left for the second slice"
	);
	buffer.complete_flush();
	assert!(buffer.take_for_flush().is_none(), "both flows must have been handed out");
}

#[test]
fn join_expiries_travel_with_their_operators_state_in_one_slice() {
	// an expiry stranded in a later slice than the state it guards is a timer that fires against rows
	// sqlite does not have yet
	let buffer = OperatorResidentState::with_budget(entry_bytes("k1", "v"));
	buffer.record_state_set(OP_A, key("k1"), row("v"), DurablePre::Absent);
	buffer.record_state_set(OP_A, key("k2"), row("v"), DurablePre::Absent);
	for row_number in 0..10u64 {
		buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(row_number), DateTime::from_millis(100));
	}

	let first = buffer.take_for_flush().expect("the seeded buffer yields a slice");
	assert_eq!(first.state.len(), 2, "the operator hands out all of its state at once, budget or not");
	assert_eq!(
		first.join_expiries.len(),
		10,
		"every armed expiry must leave with the state of the same operator, not trail it by a slice"
	);
	buffer.complete_flush();

	assert!(
		buffer.take_for_flush().is_none(),
		"nothing may be left behind; a stranded expiry never becomes durable and the timer is lost"
	);
}

#[test]
fn a_key_rewritten_while_its_flush_is_in_flight_is_counted_once() {
	let buffer = OperatorResidentState::new();
	let k = state_key("a");
	buffer.record_state_set(OP_A, k.clone(), row("v1"), DurablePre::Absent);
	buffer.take_for_flush().expect("the seeded batch must be takeable");
	buffer.record_state_set(OP_A, k.clone(), row("rewritten"), DurablePre::Absent);

	let census = buffer.census();
	assert_eq!(census.len(), 1, "one key in one keyspace is one bucket");
	assert_eq!(census[0].keys, 1, "the key is one key, however many batches happen to hold a copy of it");
	assert_eq!(
		census[0].key_bytes,
		ByteSize::from_bytes(k.len() as u64),
		"the key is billed once, not once per resident batch"
	);
	assert_eq!(
		census[0].value_bytes,
		ByteSize::from_bytes(row("rewritten").bytes().len() as u64),
		"the live rewrite is the row that stands, so its size is the one billed"
	);
	assert_eq!(
		buffer.total_bytes(),
		ByteSize::from_bytes((k.len() + row("rewritten").bytes().len()) as u64),
		"total bytes must agree with the census it is derived from"
	);
	assert_eq!(buffer.bytes(OP_A), buffer.total_bytes(), "one operator holds everything here");
}

#[test]
fn a_key_removed_while_its_flush_is_in_flight_is_not_counted_at_all() {
	let buffer = OperatorResidentState::new();
	let k = state_key("a");
	buffer.record_state_set(OP_A, k.clone(), row("v1"), DurablePre::Absent);
	buffer.take_for_flush().expect("the seeded batch must be takeable");
	buffer.record_state_remove(OP_A, k.clone(), DurablePre::Absent);

	assert!(buffer.census().is_empty(), "a key the newest batch tombstones is gone, not merely shadowed");
	assert_eq!(buffer.total_bytes(), ByteSize::ZERO, "and it bills nothing");
}

#[test]
fn a_join_expiry_rearmed_while_its_flush_is_in_flight_is_counted_once() {
	let buffer = OperatorResidentState::new();
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::default());
	buffer.take_for_flush().expect("the seeded batch must be takeable");
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::default());

	let census = buffer.join_expiry_census();
	assert_eq!(census.len(), 1, "one operator is one bucket");
	assert_eq!(census[0].keys, 1, "one join expiry tuple is one join expiry, in however many batches it appears");
	assert_eq!(
		buffer.total_bytes(),
		JOIN_EXPIRY_KEY_BYTES + JOIN_EXPIRY_VALUE_BYTES,
		"and it bills one join expiry's worth of bytes, not two"
	);
}

#[test]
fn a_join_expiry_disarmed_while_its_flush_is_in_flight_is_not_counted() {
	let buffer = OperatorResidentState::new();
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::default());
	buffer.take_for_flush().expect("the seeded batch must be takeable");
	buffer.record_join_expiry_remove(OP_A, GROUP_A, 0, RowNumber(1));

	assert!(buffer.join_expiry_census().is_empty(), "a disarmed join expiry is gone, not merely shadowed");
	assert_eq!(buffer.total_bytes(), ByteSize::ZERO, "and it bills nothing");
}

#[test]
fn a_rewritten_key_charges_its_key_once_and_only_the_row_that_stands() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k1"), row("aaaaaaaa"), DurablePre::Absent);
	assert_eq!(live_bytes(&buffer), entry_bytes("k1", "aaaaaaaa"), "a first write charges its key and its row");

	buffer.record_state_set(OP_A, key("k1"), row("bb"), DurablePre::Absent);

	assert_eq!(
		live_bytes(&buffer),
		entry_bytes("k1", "bb"),
		"the collapse must drop the outgoing row and charge the key exactly once, otherwise every rewrite \
		 of a hot key counts twice"
	);
}

#[test]
fn a_tombstone_keeps_its_key_charged() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k1"), row("value"), DurablePre::Absent);

	buffer.record_state_remove(OP_A, key("k1"), DurablePre::Absent);

	assert_eq!(
		live_bytes(&buffer),
		ByteSize::from_bytes(key("k1").len() as u64),
		"a tombstone still holds its key in memory; charging it zero hides a keyspace that is all deletes"
	);
	assert_eq!(
		buffer.lookup_state(OP_A, &key("k1")),
		BufferedState::Tombstone,
		"the charge must come from a slot that is really still there"
	);
}

#[test]
fn a_tombstone_recorded_first_charges_its_key() {
	let buffer = OperatorResidentState::new();

	buffer.record_state_remove(OP_A, key("k1"), DurablePre::Absent);

	assert_eq!(
		live_bytes(&buffer),
		ByteSize::from_bytes(key("k1").len() as u64),
		"a delete-only key is resident too; a free tombstone lets a delete storm escape the budget"
	);
}

#[test]
fn a_join_expiry_is_charged_its_fixed_width_once_per_slot() {
	let buffer = OperatorResidentState::new();
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	assert_eq!(live_bytes(&buffer), JOIN_EXPIRY_ENTRY_BYTES, "one armed slot is one fixed-width charge");

	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(900));
	buffer.record_join_expiry_remove(OP_A, GROUP_A, 0, RowNumber(1));

	assert_eq!(
		live_bytes(&buffer),
		JOIN_EXPIRY_ENTRY_BYTES,
		"a rearm and a disarm both overwrite one slot; charging each of them again would flush on a \
		 buffer that never grew"
	);
}

#[test]
fn a_flow_boundary_split_moves_exactly_the_bytes_the_slice_carries_away() {
	// a byte counted on both sides of the split makes the budget believe it is over cap forever, and
	// one counted on neither makes it flush on a buffer that never grew
	let buffer = OperatorResidentState::with_budget(entry_bytes("k1", "aaa"));
	write_in_flow(&buffer, FLOW_A, 1, &[insert(OP_A, "k1", "aaa")]);
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	write_in_flow(&buffer, FLOW_B, 2, &[insert(OP_B, "k2", "bbbbb")]);
	let before = live_bytes(&buffer);

	let taken = buffer.take_for_flush().expect("the seeded buffer yields a slice");

	assert!(taken.bytes > ByteSize::ZERO, "a slice that carries rows must carry a charge");
	assert!(
		live_bytes(&buffer) > ByteSize::ZERO,
		"the budget must have stopped at the flow boundary and left something behind to split at all"
	);
	assert_eq!(
		taken.bytes.saturating_add(live_bytes(&buffer)),
		before,
		"every byte must land on exactly one side of the split"
	);
}

#[test]
fn a_split_that_takes_everything_leaves_the_source_at_zero() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k1"), row("aaa"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	let before = live_bytes(&buffer);

	let taken = buffer.take_for_flush().expect("the seeded buffer yields a slice");

	assert_eq!(taken.bytes, before, "a slice that took everything carries the whole charge");
	assert_eq!(
		live_bytes(&buffer),
		ByteSize::ZERO,
		"a residue left on an emptied batch never drains, so the buffer flushes on every commit forever"
	);
}

#[test]
fn a_drop_marker_releases_the_bytes_of_everything_it_clears() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k1"), row("gone"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_state_set(OP_B, key("k2"), row("stays"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_B, GROUP_B, 0, RowNumber(1), DateTime::from_millis(100));

	buffer.record_drop(DropMarker::OperatorState(OP_A));

	assert_eq!(
		live_bytes(&buffer),
		entry_bytes("k2", "stays").saturating_add(JOIN_EXPIRY_ENTRY_BYTES),
		"only the surviving operator may still be charged; a dropped operator's bytes are gone from RAM"
	);
}

#[test]
fn a_join_expiry_group_drop_releases_only_that_group() {
	let buffer = OperatorResidentState::new();
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(2), DateTime::from_millis(100));
	buffer.record_join_expiry_set(OP_A, GROUP_B, 0, RowNumber(1), DateTime::from_millis(100));

	buffer.record_drop(DropMarker::JoinExpiriesGroup(OP_A, GROUP_A));

	assert_eq!(
		live_bytes(&buffer),
		JOIN_EXPIRY_ENTRY_BYTES,
		"the two cleared join expiries must be refunded and the untouched group must stay charged"
	);
}

#[test]
fn a_selected_slice_stays_resident_until_the_flush_settles() {
	let buffer = OperatorResidentState::new();
	buffer.record_state_set(OP_A, key("k1"), row("value"), DurablePre::Absent);
	let charged = live_bytes(&buffer);

	let batch = buffer.take_for_flush().expect("the seeded buffer yields a slice");

	assert_eq!(batch.bytes, charged, "the slice carries the charge it took");
	assert_eq!(live_bytes(&buffer), ByteSize::ZERO, "the live batch has handed the entries over");
	assert_eq!(
		resident_bytes(&buffer),
		charged,
		"the in-flight slice is still held in memory, so the buffer must keep counting it"
	);

	buffer.complete_flush();

	assert_eq!(resident_bytes(&buffer), ByteSize::ZERO, "the settle is where the memory is actually given back");
}

#[test]
fn a_join_expiry_armed_and_disarmed_before_any_flush_leaves_no_entry_at_all() {
	let buffer = OperatorResidentState::new();

	buffer.apply_batch(&[OperatorWrite::JoinExpiryInsert {
		operator: OP_A,
		group: GROUP_A,
		side: 1,
		row_num: RowNumber(7),
		at: DateTime::from_millis(1_234),
	}]);
	assert_eq!(
		live_bytes(&buffer),
		JOIN_EXPIRY_ENTRY_BYTES,
		"the armed slot must be charged before the disarm, or the reclaim below proves nothing"
	);

	buffer.apply_batch(&[OperatorWrite::JoinExpiryRemove {
		operator: OP_A,
		group: GROUP_A,
		side: 1,
		row_num: RowNumber(7),
		pre: DurablePre::Absent,
	}]);

	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 1, RowNumber(7)),
		BufferedJoinExpiry::Absent,
		"a never-durable arm/disarm pair must cancel to nothing; a Tombstone here masks a sqlite row \
		 that was never written and stays resident forever"
	);
	assert_eq!(
		live_bytes(&buffer),
		ByteSize::ZERO,
		"the collapsed pair must give its bytes back, otherwise the buffer still grows without bound"
	);
	assert!(buffer.take_for_flush().is_none(), "a cancelled pair must not produce a no-op DELETE slice");
}

#[test]
fn a_join_expiry_disarmed_after_its_flush_still_leaves_a_tombstone() {
	let buffer = OperatorResidentState::new();

	buffer.apply_batch(&[OperatorWrite::JoinExpiryInsert {
		operator: OP_A,
		group: GROUP_A,
		side: 1,
		row_num: RowNumber(7),
		at: DateTime::from_millis(1_234),
	}]);
	buffer.take_for_flush().expect("the armed join expiry is the only pending write, so it forms a slice");
	buffer.complete_flush();

	buffer.apply_batch(&[OperatorWrite::JoinExpiryRemove {
		operator: OP_A,
		group: GROUP_A,
		side: 1,
		row_num: RowNumber(7),
		pre: DurablePre::Absent,
	}]);

	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 1, RowNumber(7)),
		BufferedJoinExpiry::Tombstone,
		"the flush made the row durable, so the buffer must mask it regardless of what the producer \
		 claimed about the pre-image it saw"
	);
}

#[test]
fn a_producer_claiming_a_durable_pre_image_cannot_stop_a_never_flushed_pair_from_collapsing() {
	let buffer = OperatorResidentState::new();

	buffer.apply_batch(&[OperatorWrite::JoinExpiryInsert {
		operator: OP_A,
		group: GROUP_A,
		side: 0,
		row_num: RowNumber(3),
		at: DateTime::from_millis(1_000),
	}]);

	buffer.apply_batch(&[OperatorWrite::JoinExpiryRemove {
		operator: OP_A,
		group: GROUP_A,
		side: 0,
		row_num: RowNumber(3),
		pre: DurablePre::Present(JOIN_EXPIRY_ENTRY_BYTES),
	}]);

	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 0, RowNumber(3)),
		BufferedJoinExpiry::Absent,
		"the buffer must overrule the producer: it never flushed this slot, so there is no sqlite row \
		 for a tombstone to mask"
	);
	assert_eq!(
		live_bytes(&buffer),
		ByteSize::ZERO,
		"a claim the buffer can disprove must not be able to pin bytes here"
	);
}

#[test]
fn ten_thousand_arm_disarm_cycles_leave_nothing_behind() {
	let buffer = OperatorResidentState::new();

	for row_number in 0..10_000u64 {
		buffer.apply_batch(&[OperatorWrite::JoinExpiryInsert {
			operator: OP_A,
			group: GROUP_A,
			side: 0,
			row_num: RowNumber(row_number),
			at: DateTime::from_millis(1_000 + row_number),
		}]);
		buffer.apply_batch(&[OperatorWrite::JoinExpiryRemove {
			operator: OP_A,
			group: GROUP_A,
			side: 0,
			row_num: RowNumber(row_number),
			pre: DurablePre::Present(JOIN_EXPIRY_ENTRY_BYTES),
		}]);
	}

	assert!(
		buffer.join_expiries_for_group(OP_A, GROUP_A).join_expiries.is_empty(),
		"every armed slot was disarmed, so the group scan must walk nothing; anything left here is \
		 read on every join_expiry_min the join performs"
	);
	assert_eq!(
		live_bytes(&buffer),
		ByteSize::ZERO,
		"10k completed cycles must cost no resident bytes, otherwise churn alone fills the buffer"
	);
	assert!(buffer.take_for_flush().is_none(), "and none of it may reach sqlite as a no-op DELETE");
}

#[test]
fn dropping_a_group_clears_the_durable_marks_that_would_block_a_later_collapse() {
	let buffer = OperatorResidentState::new();

	buffer.apply_batch(&[OperatorWrite::JoinExpiryInsert {
		operator: OP_A,
		group: GROUP_A,
		side: 0,
		row_num: RowNumber(5),
		at: DateTime::from_millis(1_000),
	}]);
	buffer.take_for_flush().expect("the armed join expiry forms a slice");
	buffer.complete_flush();

	buffer.record_drop(DropMarker::JoinExpiriesGroup(OP_A, GROUP_A));

	buffer.apply_batch(&[OperatorWrite::JoinExpiryInsert {
		operator: OP_A,
		group: GROUP_A,
		side: 0,
		row_num: RowNumber(5),
		at: DateTime::from_millis(2_000),
	}]);
	buffer.apply_batch(&[OperatorWrite::JoinExpiryRemove {
		operator: OP_A,
		group: GROUP_A,
		side: 0,
		row_num: RowNumber(5),
		pre: DurablePre::Present(JOIN_EXPIRY_ENTRY_BYTES),
	}]);

	assert_eq!(
		buffer.lookup_join_expiry(OP_A, GROUP_A, 0, RowNumber(5)),
		BufferedJoinExpiry::Dropped,
		"the re-armed slot must collapse to nothing and fall through to the group drop; a Tombstone \
		 here means a stale durable mark survived the drop"
	);
}

fn assert_census_holds(buffer: &OperatorResidentState, step: &str) {
	assert_eq!(buffer.census(), buffer.census_by_scan(), "the state census drifted after {step}");
	assert_eq!(
		buffer.join_expiry_census(),
		buffer.join_expiry_census_by_scan(),
		"the join expiry census drifted after {step}"
	);
}

#[test]
fn every_mutation_path_keeps_the_census_equal_to_a_fresh_scan() {
	let buffer = OperatorResidentState::new();
	let first = state_key("k1");
	let second = state_key("k2");
	let flushed = state_key("k4");
	let neighbour = state_key("k5");

	buffer.record_state_set(OP_A, first.clone(), row("v1"), DurablePre::Absent);
	assert_census_holds(&buffer, "the first write");

	buffer.record_state_set(OP_A, second.clone(), row("v2"), DurablePre::Absent);
	buffer.record_state_set(OP_A, flushed.clone(), row("v4"), DurablePre::Absent);
	assert_census_holds(&buffer, "two more writes");

	buffer.record_state_set(OP_A, first.clone(), row("v1-longer"), DurablePre::Absent);
	assert_census_holds(&buffer, "an overwrite inside the live batch");

	buffer.record_join_expiry_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_join_expiry_set(OP_A, GROUP_B, 0, RowNumber(2), DateTime::from_millis(200));
	assert_census_holds(&buffer, "two armed join expiries");

	buffer.take_for_flush().expect("the seeded batch must be takeable");
	assert_census_holds(&buffer, "the slice moving into flight");

	buffer.record_state_set(OP_A, first.clone(), row("v1-live"), DurablePre::Absent);
	assert_census_holds(&buffer, "an overwrite of a key only the in-flight batch holds");

	buffer.record_state_remove(OP_A, second.clone(), DurablePre::Absent);
	assert_census_holds(&buffer, "a tombstone over a key only the in-flight batch holds");

	buffer.record_join_expiry_remove(OP_A, GROUP_A, 0, RowNumber(1));
	assert_census_holds(&buffer, "a join expiry collapsed while its arm is in flight");

	buffer.complete_flush();
	assert_census_holds(&buffer, "the flush completing");

	buffer.record_state_set(OP_B, neighbour.clone(), row("v5"), DurablePre::Absent);
	buffer.record_join_expiry_set(OP_A, GROUP_B, 0, RowNumber(2), DateTime::from_millis(300));
	buffer.record_join_expiry_set(OP_B, GROUP_A, 0, RowNumber(3), DateTime::from_millis(400));
	assert_census_holds(&buffer, "a second operator joining");

	buffer.record_drop(DropMarker::JoinExpiriesGroup(OP_A, GROUP_B));
	assert_census_holds(&buffer, "a group join expiry drop");

	buffer.record_drop(DropMarker::OperatorState(OP_A));
	assert_census_holds(&buffer, "an operator drop");

	assert_eq!(
		buffer.census(),
		vec![OperatorStateCensus {
			operator: OP_B,
			keyspace: OperatorStateKey::decode_keyspace(0x10),
			keys: 1,
			key_bytes: ByteSize::from_bytes(neighbour.len() as u64),
			value_bytes: ByteSize::from_bytes(row("v5").bytes().len() as u64),
		}],
		"only the untouched operator's key may survive the drop"
	);
	assert_eq!(
		buffer.join_expiry_census(),
		vec![StoredJoinRowExpiryCensus {
			operator: OP_B,
			keys: 1,
		}],
		"and only the untouched operator's join expiry"
	);
}
