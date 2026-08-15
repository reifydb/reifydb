// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{QUEUE_ATTEMPT_HEADER_SIZE, RowBuilder, SHAPE_HEADER_SIZE},
	queue_attempt::EncodedQueueAttemptRow,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::{
	encoding::LeBytes,
	value::{datetime::DateTime, value_type::ValueType},
};

fn shape() -> RowShape {
	RowShape::new(
		RowFamily::QueueAttempt,
		vec![
			RowShapeField::unconstrained("worker", ValueType::Utf8),
			RowShapeField::unconstrained("response", ValueType::Utf8),
			RowShapeField::unconstrained("anomaly", ValueType::Utf8),
		],
	)
}

#[test]
fn the_attempt_header_is_the_source_header_plus_outcome_lost_and_finished_at() {
	// Any other width silently reinterprets every field offset in every stored attempt row.
	assert_eq!(QUEUE_ATTEMPT_HEADER_SIZE, SHAPE_HEADER_SIZE + 1 + 1 + DateTime::ENCODED_SIZE);
	assert_eq!(QUEUE_ATTEMPT_HEADER_SIZE, 43);
	assert_eq!(shape().header_size(), QUEUE_ATTEMPT_HEADER_SIZE);
}

#[test]
fn the_bitvec_starts_after_finished_at_so_fields_do_not_alias_the_instant() {
	// At a 33-byte offset the bitvec would sit inside outcome and read the tag as validity bits.
	let shape = shape();

	assert_eq!(shape.data_offset(), QUEUE_ATTEMPT_HEADER_SIZE + shape.bitvec_size());
	assert_eq!(shape.fields()[0].offset as usize, QUEUE_ATTEMPT_HEADER_SIZE + shape.bitvec_size());
}

#[test]
fn the_three_header_facts_survive_a_freeze() {
	// A slot lost on freeze would make a failed attempt read as a successful one.
	let mut row = shape().allocate_queue_attempt();
	row.set_outcome(2);
	row.set_lost(true);
	row.set_finished_at(DateTime::from_nanos(1_234_567_890));

	let frozen = row.freeze();

	assert_eq!(frozen.outcome(), 2);
	assert!(frozen.lost());
	assert_eq!(frozen.finished_at(), DateTime::from_nanos(1_234_567_890));
}

#[test]
fn writing_a_body_field_leaves_the_header_facts_untouched() {
	// worker is variable-length, so a body write landing at a header offset would overwrite outcome.
	let shape = shape();
	let mut row = shape.allocate_queue_attempt();
	row.set_outcome(1);
	row.set_lost(false);
	row.set_finished_at(DateTime::from_nanos(99));

	shape.set_utf8(&mut row, 0, "worker-with-a-deliberately-long-name");
	shape.set_utf8(&mut row, 1, "a response body");

	let frozen = row.freeze();

	assert_eq!(frozen.outcome(), 1);
	assert!(!frozen.lost());
	assert_eq!(frozen.finished_at(), DateTime::from_nanos(99));
	assert_eq!(shape.get_utf8(frozen.as_slice(), 0), "worker-with-a-deliberately-long-name");
}

#[test]
fn a_view_over_raw_bytes_reads_the_same_header_facts() {
	// A view that disagreed with the builder would decode stored attempts differently from written ones.
	let mut row = shape().allocate_queue_attempt();
	row.set_outcome(2);
	row.set_lost(true);
	row.set_finished_at(DateTime::from_nanos(7));
	let bytes = row.freeze_bytes();

	let viewed = EncodedQueueAttemptRow::view(&bytes);

	assert_eq!(viewed.outcome(), 2);
	assert!(viewed.lost());
	assert_eq!(viewed.finished_at(), DateTime::from_nanos(7));
}

#[test]
fn the_header_facts_are_always_present_and_never_absent() {
	// These carry no flag bit, so a fresh row must read as a concrete zero rather than as unset.
	let row = shape().allocate_queue_attempt().freeze();

	assert_eq!(row.outcome(), 0);
	assert!(!row.lost());
	assert_eq!(row.finished_at(), DateTime::from_nanos(0));
}

#[test]
fn a_freshly_allocated_row_already_carries_the_shape_it_was_allocated_from() {
	// allocate reaches this family only through its catch-all arm, so narrowing that arm would leave the row unresolvable.
	let shape = shape();

	let row = shape.allocate_queue_attempt().freeze();

	assert_eq!(row.fingerprint(), shape.fingerprint());
	assert_ne!(row.fingerprint(), RowShape::new(RowFamily::Queue, vec![]).fingerprint());
}

#[test]
fn the_wall_clock_stamps_survive_a_freeze_and_stay_disjoint() {
	// Retention sweeps on updated_at, so one shared stamp slot makes every attempt look either immortal or already expired.
	let mut row = shape().allocate_queue_attempt();
	row.set_timestamps(DateTime::from_nanos(11), DateTime::from_nanos(22));

	let frozen = row.freeze();

	assert_eq!(frozen.created_at(), DateTime::from_nanos(11));
	assert_eq!(frozen.updated_at(), DateTime::from_nanos(22));
}

#[test]
fn a_row_handed_to_storage_and_read_back_is_the_same_row() {
	// Every write leaves through into_bytes and every read arrives through From, so an asymmetry there loses the header.
	let mut row = shape().allocate_queue_attempt();
	row.set_outcome(2);
	row.set_finished_at(DateTime::from_nanos(7));
	let original = row.freeze();

	let restored = EncodedQueueAttemptRow::from(original.clone().into_bytes());

	assert_eq!(restored, original);
	assert_eq!(restored.outcome(), 2);
	assert_eq!(restored.finished_at(), DateTime::from_nanos(7));
}

#[test]
fn an_optional_body_field_that_was_never_set_reads_back_as_none() {
	// A live ack carries no response, and an empty string there would read as a response that was actually recorded.
	let shape = shape();
	let mut row = shape.allocate_queue_attempt();
	shape.set_utf8(&mut row, 0, "worker-1");

	let frozen = row.freeze();

	assert_eq!(shape.get_utf8(frozen.as_slice(), 0), "worker-1");
	assert_eq!(shape.try_get_utf8(frozen.as_slice(), 1), None, "an unset response must not read as empty");
	assert_eq!(shape.try_get_utf8(frozen.as_slice(), 2), None, "an unset anomaly must not read as empty");
}

#[test]
fn thawing_an_open_attempt_closes_it_without_rewriting_the_body() {
	// The three facts sit below the body, so a drifting header offset would corrupt the worker text on close.
	let shape = shape();
	let mut row = shape.allocate_queue_attempt();
	row.set_timestamps(DateTime::from_nanos(1), DateTime::from_nanos(1));
	shape.set_utf8(&mut row, 0, "worker-7");
	shape.set_utf8(&mut row, 1, "still running");
	let frozen = row.freeze();
	assert_eq!(frozen.outcome(), 0, "the fixture must start as an open attempt");
	let body_before = frozen.body().to_vec();

	let mut thawed = frozen.thaw();
	thawed.set_outcome(2);
	thawed.set_lost(true);
	thawed.set_finished_at(DateTime::from_nanos(9_000));
	let refrozen = thawed.freeze();

	assert_eq!(refrozen.outcome(), 2);
	assert!(refrozen.lost());
	assert_eq!(refrozen.finished_at(), DateTime::from_nanos(9_000));
	assert_eq!(refrozen.created_at(), DateTime::from_nanos(1), "closing must not restamp the attempt");
	assert_eq!(refrozen.body(), body_before.as_slice(), "closing an attempt must not move a body byte");
	assert_eq!(shape.get_utf8(refrozen.as_slice(), 0), "worker-7");
}

#[test]
#[should_panic(expected = "allocate_queue_attempt on a shape of another family")]
fn a_shape_of_another_family_cannot_allocate_an_attempt_row() {
	// Allocating across families must panic, otherwise the declared shape and the layout disagree.
	RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("worker", ValueType::Utf8)])
		.allocate_queue_attempt();
}
