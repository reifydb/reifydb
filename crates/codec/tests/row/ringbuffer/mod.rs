// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{RINGBUFFER_HEADER_SIZE, RowBuilder, SHAPE_HEADER_SIZE},
	ringbuffer::EncodedRingBufferRow,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::value::{datetime::DateTime, value_type::ValueType};

fn shape() -> RowShape {
	RowShape::new(
		RowFamily::RingBuffer,
		vec![
			RowShapeField::unconstrained("id", ValueType::Int4),
			RowShapeField::unconstrained("payload", ValueType::Int4),
		],
	)
}

#[test]
fn the_ringbuffer_header_is_the_source_header_plus_the_sequence() {
	// Borrowing the plain source width would read the #sequence bytes as the bitvec and shift every field.
	assert_eq!(shape().header_size(), RINGBUFFER_HEADER_SIZE);
	assert_eq!(shape().header_size(), SHAPE_HEADER_SIZE + 8);
	assert_eq!(shape().header_size(), 41);
}

#[test]
fn the_sequence_round_trips_and_leaves_the_stamps_and_fields_alone() {
	// The sequence slot sits between the flags byte and the bitvec, so a wrong offset clobbers a stamp or a field.
	let shape = shape();
	let mut row = shape.allocate_ringbuffer();
	row.set_timestamps(DateTime::from_millis(111), DateTime::from_millis(222));
	row.set_time(DateTime::from_millis(333));
	shape.set::<i32>(&mut row, 0, 7);
	shape.set::<i32>(&mut row, 1, -7);
	row.set_sequence(u64::MAX - 1);

	let frozen = row.freeze_bytes();
	let typed = EncodedRingBufferRow::view(&frozen);

	assert_eq!(typed.sequence(), u64::MAX - 1);
	assert_eq!(shape.sequence(&frozen), u64::MAX - 1);
	assert_eq!(typed.created_at(), DateTime::from_millis(111));
	assert_eq!(typed.updated_at(), DateTime::from_millis(222));
	assert_eq!(typed.time(), Some(DateTime::from_millis(333)));
	assert_eq!(shape.get::<i32>(&frozen, 0), 7);
	assert_eq!(shape.get::<i32>(&frozen, 1), -7);
}

#[test]
fn the_typed_view_reads_the_stamps_the_builder_wrote() {
	// The stamps are adjacent 8-byte slots, so swapped offsets round trip undetected unless the values differ.
	let shape = shape();
	let mut row = shape.allocate_ringbuffer();
	row.set_timestamps(DateTime::from_millis(111), DateTime::from_millis(222));
	row.set_time(DateTime::from_millis(333));

	let frozen = row.freeze_bytes();
	let typed = EncodedRingBufferRow::view(&frozen);

	assert_eq!(typed.created_at(), DateTime::from_millis(111));
	assert_eq!(typed.updated_at(), DateTime::from_millis(222));
	assert_eq!(typed.time(), Some(DateTime::from_millis(333)));
	assert_eq!(typed.fingerprint(), shape.fingerprint());
}

#[test]
fn a_slot_rewrite_moves_updated_at_and_never_created_at() {
	// Eviction rewrites a slot in place, so one shared stamp slot makes an overwritten row look freshly inserted.
	let shape = shape();
	let mut row = shape.allocate_ringbuffer();
	row.set_timestamps(DateTime::from_millis(10), DateTime::from_millis(10));

	let created_at = row.created_at();
	row.set_timestamps(created_at, DateTime::from_millis(99));

	let frozen = row.freeze_bytes();
	let typed = EncodedRingBufferRow::view(&frozen);

	assert_eq!(typed.created_at(), DateTime::from_millis(10));
	assert_eq!(typed.updated_at(), DateTime::from_millis(99));
}

#[test]
fn the_typed_definedness_agrees_with_the_shape() {
	// A header width disagreeing with the shape's reads a field byte as a validity bit.
	let shape = shape();
	let mut row = shape.allocate_ringbuffer();
	shape.set::<i32>(&mut row, 1, 9);

	let frozen = row.freeze_bytes();
	let typed = EncodedRingBufferRow::view(&frozen);

	for index in 0..shape.field_count() {
		assert_eq!(typed.is_defined(index), shape.is_defined(&frozen, index), "field {index} disagrees");
	}
	assert!(typed.is_defined(1));
	assert!(!typed.is_defined(0));
}

#[test]
fn the_body_begins_where_the_bitvec_begins() {
	// body() slices at its own constant, so a wrong one hands out header bytes as row content.
	let shape = shape();
	let mut row = shape.allocate_ringbuffer();
	shape.set::<i32>(&mut row, 0, 7);

	let frozen = row.freeze_bytes();
	let typed = EncodedRingBufferRow::view(&frozen);

	assert_eq!(typed.body().len(), frozen.len() - RINGBUFFER_HEADER_SIZE);
	assert_eq!(typed.body(), &frozen.as_slice()[shape.header_size()..]);
}

#[test]
fn thawing_an_evicted_slot_moves_updated_at_and_leaves_created_at_alone() {
	// Eviction rewrites a slot in place, so one shared stamp slot makes an overwritten row look freshly inserted.
	let shape = shape();
	let mut row = shape.allocate_ringbuffer();
	row.set_timestamps(DateTime::from_millis(10), DateTime::from_millis(10));
	shape.set::<i32>(&mut row, 0, 1);
	shape.set::<i32>(&mut row, 1, 2);
	let frozen = row.freeze();

	let mut thawed = frozen.thaw();
	let created_at = thawed.created_at();
	thawed.set_timestamps(created_at, DateTime::from_millis(99));
	shape.set::<i32>(&mut thawed, 1, 3);
	let refrozen = thawed.freeze();

	assert_eq!(refrozen.created_at(), DateTime::from_millis(10));
	assert_eq!(refrozen.updated_at(), DateTime::from_millis(99));
	assert_eq!(shape.get::<i32>(refrozen.as_slice(), 0), 1, "an untouched column must survive the rewrite");
	assert_eq!(shape.get::<i32>(refrozen.as_slice(), 1), 3);
	assert_eq!(refrozen.fingerprint(), shape.fingerprint());
}

#[test]
#[should_panic(expected = "allocate_ringbuffer on a shape of another family")]
fn a_shape_of_another_family_cannot_allocate_a_ringbuffer_row() {
	// A table shape is the same width and the same layout, so the row would differ only in its fingerprint.
	RowShape::new(RowFamily::Table, vec![RowShapeField::unconstrained("id", ValueType::Int4)])
		.allocate_ringbuffer();
}
