// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{QUEUE_HEADER_SIZE, RowBuilder, SHAPE_HEADER_SIZE},
	queue::EncodedQueueRow,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::{
	encoding::LeBytes,
	value::{datetime::DateTime, value_type::ValueType},
};

fn shape() -> RowShape {
	RowShape::new(
		RowFamily::Queue,
		vec![
			RowShapeField::unconstrained("id", ValueType::Int4),
			RowShapeField::unconstrained("payload", ValueType::Int4),
		],
	)
}

#[test]
fn the_queue_header_is_the_source_header_plus_one_instant() {
	// Any other width silently reinterprets every field offset in every stored queue row.
	assert_eq!(QUEUE_HEADER_SIZE, SHAPE_HEADER_SIZE + DateTime::ENCODED_SIZE);
	assert_eq!(QUEUE_HEADER_SIZE, 41);
	assert_eq!(shape().header_size(), QUEUE_HEADER_SIZE);
}

#[test]
fn the_bitvec_starts_after_not_before_so_fields_do_not_alias_the_instant() {
	// At a 33-byte offset the bitvec would sit inside not_before and read its bytes as validity bits.
	let shape = shape();

	assert_eq!(shape.data_offset(), QUEUE_HEADER_SIZE + shape.bitvec_size());
	assert_eq!(shape.fields()[0].offset as usize, QUEUE_HEADER_SIZE + shape.bitvec_size());
}

#[test]
fn an_absent_not_before_is_distinguishable_from_a_present_one() {
	// Absence means due-now, so a sentinel instant would make the earliest schedulable item unrepresentable.
	let shape = shape();
	let row = shape.allocate_queue().freeze_bytes();

	assert_eq!(EncodedQueueRow::view(&row).not_before(), None);
}

#[test]
fn not_before_round_trips_through_the_header() {
	let shape = shape();
	let instant = DateTime::from_millis(1_700_000_000_000);
	let mut row = shape.allocate_queue();
	row.set_not_before(instant);

	assert_eq!(EncodedQueueRow::view(&row.freeze_bytes()).not_before(), Some(instant));
}

#[test]
fn stamping_not_before_must_not_define_any_field() {
	// not_before shares the flags byte with #time, so a stray bit here reads a field as present.
	let shape = shape();
	let mut row = shape.allocate_queue();
	row.set_not_before(DateTime::from_millis(1));

	let frozen = row.freeze_bytes();
	for index in 0..shape.field_count() {
		assert!(!shape.is_defined(&frozen, index), "field {index} must stay undefined");
	}
}

#[test]
fn defining_a_field_must_not_disturb_not_before() {
	// The field bitvec begins one byte past the instant, so an off-by-one write lands in its last byte.
	let shape = shape();
	let instant = DateTime::from_millis(1_700_000_000_000);
	let mut row = shape.allocate_queue();
	row.set_not_before(instant);
	shape.set::<i32>(&mut row, 0, 7);
	shape.set::<i32>(&mut row, 1, 9);

	let frozen = row.freeze_bytes();
	assert_eq!(EncodedQueueRow::view(&frozen).not_before(), Some(instant));
	assert_eq!(shape.get::<i32>(&frozen, 0), 7);
	assert_eq!(shape.get::<i32>(&frozen, 1), 9);
}

#[test]
fn time_and_not_before_occupy_independent_flag_bits() {
	// Both live in the same flags byte, so one setter clobbering the other's bit loses a schedule.
	let shape = shape();
	let time = DateTime::from_millis(111);
	let not_before = DateTime::from_millis(222);
	let mut row = shape.allocate_queue();
	row.set_time(time);
	row.set_not_before(not_before);

	let frozen = row.freeze_bytes();
	assert_eq!(shape.time(&frozen), Some(time));
	assert_eq!(EncodedQueueRow::view(&frozen).not_before(), Some(not_before));
}

#[test]
fn thawing_a_frozen_row_reschedules_it_without_disturbing_the_rest_of_the_header() {
	// not_before shares its flags byte with the event time, so a retry clearing the wrong bit would lose that time.
	let shape = shape();
	let mut row = shape.allocate_queue();
	row.set_timestamps(DateTime::from_millis(1), DateTime::from_millis(1));
	row.set_time(DateTime::from_millis(500));
	row.set_not_before(DateTime::from_millis(1_000));
	shape.set::<i32>(&mut row, 0, 7);
	shape.set::<i32>(&mut row, 1, 9);
	let frozen = row.freeze();

	let mut thawed = frozen.thaw();
	assert_eq!(thawed.not_before(), Some(DateTime::from_millis(1_000)), "the schedule must survive the thaw");
	thawed.set_not_before(DateTime::from_millis(2_000));
	let refrozen = thawed.freeze();

	assert_eq!(refrozen.not_before(), Some(DateTime::from_millis(2_000)));
	assert_eq!(refrozen.created_at(), DateTime::from_millis(1));
	assert_eq!(refrozen.updated_at(), DateTime::from_millis(1));
	assert_eq!(
		shape.time(refrozen.as_slice()),
		Some(DateTime::from_millis(500)),
		"the event time must stay flagged"
	);
	assert_eq!(shape.get::<i32>(refrozen.as_slice(), 0), 7);
	assert_eq!(shape.get::<i32>(refrozen.as_slice(), 1), 9);
}

#[test]
#[should_panic(expected = "allocate_queue on a shape of another family")]
fn a_shape_of_another_family_cannot_allocate_a_queue_row() {
	// An attempt shape is two bytes wider, so its bitvec would sit inside the slot queue reads as not_before.
	RowShape::new(RowFamily::QueueAttempt, vec![RowShapeField::unconstrained("id", ValueType::Int4)])
		.allocate_queue();
}
