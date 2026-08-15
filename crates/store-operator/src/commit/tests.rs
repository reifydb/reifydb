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

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::GroupId,
};
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

use crate::{
	commit::{OperatorCommitBuffer, batch::DropMarker},
	types::{BufferedAnchor, BufferedState, OperatorWrite},
};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn key(bytes: &str) -> EncodedKey {
	EncodedKey::new(bytes.as_bytes())
}

fn row(body: &str) -> EncodedOperatorRow {
	EncodedOperatorRow::timeless(body.as_bytes())
}

fn body(entry: &Option<EncodedOperatorRow>) -> String {
	row_body(entry.as_ref().expect("entry must carry a row"))
}

fn row_body(row: &EncodedOperatorRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

#[test]
fn a_removed_key_reads_back_as_a_tombstone_not_as_absent() {
	let buffer = OperatorCommitBuffer::new();

	assert_eq!(
		buffer.lookup_state(OP_A, &key("k")),
		BufferedState::Absent,
		"a key no layer has seen must report as unknown so the read continues to sqlite"
	);

	buffer.record_state_set(OP_A, key("k"), row("v"));
	let BufferedState::Row(found) = buffer.lookup_state(OP_A, &key("k")) else {
		panic!("the live layer knows the key it just wrote")
	};
	assert_eq!(row_body(&found), "v", "the buffer must hand back the row that was written, not a stale one");

	buffer.record_state_remove(OP_A, key("k"));
	assert_eq!(
		buffer.lookup_state(OP_A, &key("k")),
		BufferedState::Tombstone,
		"a removed key must read as a tombstone; reporting it unknown would send the read to sqlite \
		 and resurrect the deleted row"
	);
}

#[test]
fn checkpoints_distinguish_a_delete_from_a_never_written_flow() {
	let buffer = OperatorCommitBuffer::new();

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
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key("k"), row("v"));
	buffer.record_state_remove(OP_A, key("gone"));
	buffer.record_anchor_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(500));
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
		buffer.lookup_anchor(OP_A, GROUP_A, 0, RowNumber(1)),
		BufferedAnchor::Expiry(500),
		"anchors ride the same in-flight layer as state"
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
	assert_eq!(buffer.lookup_anchor(OP_A, GROUP_A, 0, RowNumber(1)), BufferedAnchor::Absent);
	assert!(buffer.lookup_checkpoint(FlowId(3)).is_none());
}

#[test]
fn a_live_write_shadows_the_same_key_in_the_in_flight_batch() {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key("k"), row("old"));
	buffer.record_state_set(OP_A, key("doomed"), row("old"));
	buffer.record_anchor_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_state_set(OP_A, key("k"), row("new"));
	buffer.record_state_remove(OP_A, key("doomed"));
	buffer.record_anchor_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(900));

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
		buffer.lookup_anchor(OP_A, GROUP_A, 0, RowNumber(1)),
		BufferedAnchor::Expiry(900),
		"the newer expiry must win, otherwise the seal fires against a superseded deadline"
	);
}

#[test]
fn state_range_is_ordered_operator_scoped_and_overlays_the_in_flight_batch() {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key("a"), row("flushing-a"));
	buffer.record_state_set(OP_A, key("b"), row("flushing-b"));
	buffer.record_state_set(OP_A, key("c"), row("flushing-c"));
	buffer.record_state_set(OP_B, key("b"), row("other-operator"));
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_state_set(OP_A, key("b"), row("live-b"));
	buffer.record_state_remove(OP_A, key("c"));
	buffer.record_state_set(OP_A, key("d"), row("live-d"));

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

fn seeded_two_layer_buffer() -> OperatorCommitBuffer {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key("a"), row("flushing-a"));
	buffer.record_state_set(OP_A, key("b"), row("flushing-b"));
	buffer.record_state_set(OP_A, key("c"), row("flushing-c"));
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_state_set(OP_A, key("b"), row("live-b"));
	buffer.record_state_set(OP_A, key("d"), row("live-d"));
	buffer
}

fn seeded_two_layer_buffer_with_dropped_operator() -> OperatorCommitBuffer {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_drop(DropMarker::OperatorState(OP_A));
	buffer.record_state_set(OP_A, key("a"), row("flushing-a"));
	buffer.record_state_set(OP_A, key("b"), row("flushing-b"));
	buffer.record_state_set(OP_A, key("c"), row("flushing-c"));
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_state_set(OP_A, key("b"), row("live-b"));
	buffer.record_state_set(OP_A, key("d"), row("live-d"));
	buffer
}

#[test]
fn anchors_for_group_overlays_the_in_flight_batch_and_keeps_tombstones() {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_anchor_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_anchor_set(OP_A, GROUP_A, 0, RowNumber(2), DateTime::from_millis(200));
	buffer.record_anchor_set(OP_A, GROUP_B, 0, RowNumber(3), DateTime::from_millis(300));
	buffer.record_anchor_set(OP_B, GROUP_A, 0, RowNumber(4), DateTime::from_millis(400));
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	buffer.record_anchor_set(OP_A, GROUP_A, 0, RowNumber(2), DateTime::from_millis(250));
	buffer.record_anchor_remove(OP_A, GROUP_A, 1, RowNumber(5));

	let anchors = buffer.anchors_for_group(OP_A, GROUP_A).anchors;
	assert_eq!(
		anchors,
		vec![((0u8, RowNumber(1)), Some(100)), ((0u8, RowNumber(2)), Some(250)), ((1u8, RowNumber(5)), None),],
		"the scan must stay inside one operator and group, overlay the live expiry, and keep the \
		 tombstone so the sqlite merge never re-arms a removed anchor"
	);
}

#[test]
fn a_drop_clears_what_came_before_it_and_keeps_what_came_after() {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key("before"), row("v"));
	buffer.record_anchor_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_state_set(OP_B, key("untouched"), row("v"));
	buffer.record_anchor_set(OP_B, GROUP_A, 0, RowNumber(2), DateTime::from_millis(200));

	buffer.record_drop(DropMarker::OperatorState(OP_A));

	buffer.record_state_set(OP_A, key("after"), row("v"));

	assert_eq!(
		buffer.lookup_state(OP_A, &key("before")),
		BufferedState::Dropped,
		"a write the drop erased must never be replayed into sqlite behind the drop"
	);
	assert_eq!(
		buffer.lookup_anchor(OP_A, GROUP_A, 0, RowNumber(1)),
		BufferedAnchor::Dropped,
		"dropping operator state takes that operator's anchors with it"
	);
	assert!(
		matches!(buffer.lookup_state(OP_A, &key("after")), BufferedState::Row(_)),
		"a write recorded after the drop must survive it, otherwise a recreated operator loses state"
	);
	assert!(
		matches!(buffer.lookup_state(OP_B, &key("untouched")), BufferedState::Row(_)),
		"the drop is scoped to one operator"
	);
	assert!(matches!(buffer.lookup_anchor(OP_B, GROUP_A, 0, RowNumber(2)), BufferedAnchor::Expiry(_)));

	let batch = buffer.take_for_flush().expect("the batch carries the marker and the later write");
	assert_eq!(
		batch.drops,
		vec![DropMarker::OperatorState(OP_A)],
		"the marker must reach the flusher; clearing memory alone leaves the sqlite rows behind"
	);
	assert_eq!(batch.state.len(), 2, "only the post-drop write and the other operator's write remain");
}

#[test]
fn an_anchor_drop_clears_only_the_anchors_it_names() {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key("k"), row("v"));
	buffer.record_anchor_set(OP_A, GROUP_A, 0, RowNumber(1), DateTime::from_millis(100));
	buffer.record_anchor_set(OP_A, GROUP_B, 0, RowNumber(2), DateTime::from_millis(200));

	buffer.record_drop(DropMarker::AnchorsGroup(OP_A, GROUP_A));

	assert_eq!(
		buffer.lookup_anchor(OP_A, GROUP_A, 0, RowNumber(1)),
		BufferedAnchor::Dropped,
		"the named group's anchors must be gone"
	);
	assert!(
		matches!(buffer.lookup_anchor(OP_A, GROUP_B, 0, RowNumber(2)), BufferedAnchor::Expiry(_)),
		"a sibling group keeps its anchors, otherwise one group's seal wipes another's timers"
	);
	assert!(
		matches!(buffer.lookup_state(OP_A, &key("k")), BufferedState::Row(_)),
		"an anchor drop must never touch operator state"
	);

	buffer.record_drop(DropMarker::AnchorsOperator(OP_A));

	assert_eq!(
		buffer.lookup_anchor(OP_A, GROUP_B, 0, RowNumber(2)),
		BufferedAnchor::Dropped,
		"an operator-wide anchor drop covers every group"
	);
	assert!(
		matches!(buffer.lookup_state(OP_A, &key("k")), BufferedState::Row(_)),
		"an operator-wide anchor drop still leaves the state alone"
	);
}

#[test]
fn take_for_flush_on_an_empty_buffer_returns_none_and_leaves_flushing_clear() {
	let buffer = OperatorCommitBuffer::new();

	assert!(buffer.take_for_flush().is_none(), "an empty tick must not open a transaction");

	let inner = buffer.shared.inner.lock();
	assert!(
		!inner.flushing,
		"a refused take must leave the flag clear, otherwise every later drop blocks forever on a \
		 flush that never runs"
	);
	assert!(inner.in_flight.is_none(), "nothing was taken, so there is no in-flight layer to read through");
}

#[test]
fn a_buffer_holding_only_a_drop_is_still_worth_flushing() {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_drop(DropMarker::OperatorState(OP_A));

	let batch = buffer
		.take_for_flush()
		.expect("a drop with no writes must still flush; the rows it erases live in sqlite");
	assert_eq!(batch.drops, vec![DropMarker::OperatorState(OP_A)]);
}

#[test]
fn take_for_flush_sets_flushing_and_complete_flush_clears_it() {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key("k"), row("v"));
	buffer.take_for_flush().expect("the seeded batch must be takeable");

	{
		let inner = buffer.shared.inner.lock();
		assert!(inner.flushing, "a taken batch must mark the buffer flushing so drops wait it out");
		assert!(inner.in_flight.is_some(), "the taken batch stays readable while the flush runs");
	}

	buffer.complete_flush();

	let inner = buffer.shared.inner.lock();
	assert!(!inner.flushing, "a completed flush must release waiting drops");
	assert!(inner.in_flight.is_none(), "the flushed batch now lives in sqlite and must not be read twice");
}

#[test]
fn a_drop_waits_out_an_in_flight_flush_before_clearing() {
	let buffer = OperatorCommitBuffer::new();
	buffer.record_state_set(OP_A, key("k"), row("v"));
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
	let buffer = OperatorCommitBuffer::new();
	buffer.apply_batch(&[
		OperatorWrite::Set {
			operator: OP_A,
			key: key("set"),
			row: row("v"),
		},
		OperatorWrite::Remove {
			operator: OP_A,
			key: key("removed"),
		},
		OperatorWrite::AnchorSet {
			operator: OP_A,
			group: GROUP_A,
			side: 1,
			row_num: RowNumber(7),
			expiry: DateTime::from_millis(1_234),
		},
		OperatorWrite::AnchorRemove {
			operator: OP_A,
			group: GROUP_A,
			side: 1,
			run_num: RowNumber(8),
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
		buffer.lookup_anchor(OP_A, GROUP_A, 1, RowNumber(7)),
		BufferedAnchor::Expiry(1_234),
		"an AnchorSet is stored as millis, matching the memory tier and the sqlite column"
	);
	assert_eq!(
		buffer.lookup_anchor(OP_A, GROUP_A, 1, RowNumber(8)),
		BufferedAnchor::Tombstone,
		"an AnchorRemove must tombstone the slot so the sqlite anchor is not read back as live"
	);
}

#[test]
fn a_combined_apply_lands_the_state_and_the_checkpoints_in_one_taken_batch() {
	let buffer = OperatorCommitBuffer::new();

	buffer.apply_batch_with_checkpoints(
		&[
			OperatorWrite::Set {
				operator: OP_A,
				key: key("state"),
				row: row("v"),
			},
			OperatorWrite::AnchorSet {
				operator: OP_A,
				group: GROUP_A,
				side: 0,
				row_num: RowNumber(1),
				expiry: DateTime::from_millis(700),
			},
		],
		&[(FlowId(3), CommitVersion(12))],
		&[FlowId(4)],
	);

	let batch = buffer.take_for_flush().expect("the combined apply must dirty the buffer");

	assert_eq!(
		body(batch.state.get(&(OP_A, key("state"))).expect("the state write must be in the batch")),
		"v",
		"the state of the committed slice must ride the same batch as its checkpoint"
	);
	assert_eq!(
		batch.anchors.get(&(OP_A, GROUP_A, 0, RowNumber(1))).copied(),
		Some(Some(700)),
		"anchors are part of the same slice, so they must not be split from the checkpoint either"
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
	let buffer = OperatorCommitBuffer::new();

	buffer.apply_batch_with_checkpoints(&[], &[], &[]);

	assert!(
		buffer.take_for_flush().is_none(),
		"a slice that changed nothing must not dirty the buffer, otherwise every idle commit opens a \
		 sqlite transaction"
	);
}

#[test]
fn an_empty_write_batch_leaves_the_buffer_untouched() {
	let buffer = OperatorCommitBuffer::new();
	buffer.apply_batch(&[]);

	assert!(
		buffer.take_for_flush().is_none(),
		"an empty apply must not dirty the buffer, otherwise every idle tick opens a transaction"
	);
}
