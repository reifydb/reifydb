// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{RowBuilder, SHAPE_HEADER_SIZE},
	shape::{RowFamily, RowShape, RowShapeField},
	table::EncodedTableRow,
};
use reifydb_value::value::{datetime::DateTime, value_type::ValueType};

fn shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			RowShapeField::unconstrained("id", ValueType::Int4),
			RowShapeField::unconstrained("payload", ValueType::Int4),
		],
	)
}

#[test]
fn the_table_header_is_the_full_source_header() {
	// The queue family sits 8 bytes wider, so borrowing its constant shifts every field offset.
	assert_eq!(shape().header_size(), SHAPE_HEADER_SIZE);
	assert_eq!(shape().header_size(), 33);
}

#[test]
fn the_typed_view_reads_the_stamps_the_builder_wrote() {
	// The stamps are adjacent 8-byte slots, so swapped offsets round trip undetected unless the values differ.
	let shape = shape();
	let mut row = shape.allocate_table();
	row.set_timestamps(DateTime::from_millis(111), DateTime::from_millis(222));
	row.set_time(DateTime::from_millis(333));

	let frozen = row.freeze_bytes();
	let typed = EncodedTableRow::view(&frozen);

	assert_eq!(typed.created_at(), DateTime::from_millis(111));
	assert_eq!(typed.updated_at(), DateTime::from_millis(222));
	assert_eq!(typed.time(), Some(DateTime::from_millis(333)));
	assert_eq!(typed.fingerprint(), shape.fingerprint());
}

#[test]
fn an_unstamped_row_reports_no_time() {
	// Absence must be the flags bit, never a zero value, or a time-less table reports an epoch it never wrote.
	let frozen = shape().allocate_table().freeze_bytes();

	assert_eq!(EncodedTableRow::view(&frozen).time(), None);
}

#[test]
fn the_typed_definedness_agrees_with_the_shape() {
	// A header width disagreeing with the shape's reads a field byte as a validity bit.
	let shape = shape();
	let mut row = shape.allocate_table();
	shape.set::<i32>(&mut row, 1, 9);

	let frozen = row.freeze_bytes();
	let typed = EncodedTableRow::view(&frozen);

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
	let mut row = shape.allocate_table();
	shape.set::<i32>(&mut row, 0, 7);

	let frozen = row.freeze_bytes();
	let typed = EncodedTableRow::view(&frozen);

	assert_eq!(typed.body().len(), frozen.len() - SHAPE_HEADER_SIZE);
	assert_eq!(typed.body(), &frozen.as_slice()[shape.header_size()..]);
}

#[test]
fn viewing_and_converting_preserve_the_bytes() {
	// view() must be a pointer cast and From a move, otherwise a typed read disagrees with the bytes it borrowed.
	let shape = shape();
	let mut row = shape.allocate_table();
	shape.set::<i32>(&mut row, 0, 42);
	row.set_timestamps(DateTime::from_millis(5), DateTime::from_millis(6));

	let frozen = row.freeze_bytes();
	let expected = frozen.as_slice().to_vec();

	assert_eq!(EncodedTableRow::view(&frozen).as_slice(), expected.as_slice());
	assert_eq!(EncodedTableRow::from(frozen.clone()).into_bytes(), frozen);
}

#[test]
fn thawing_a_frozen_row_hands_back_every_byte_it_was_frozen_from() {
	// A thaw that reallocated or re-zeroed the header would reset a stored row's shape id on every update.
	let shape = shape();
	let mut row = shape.allocate_table();
	row.set_timestamps(DateTime::from_millis(10), DateTime::from_millis(10));
	row.set_time(DateTime::from_millis(333));
	shape.set::<i32>(&mut row, 0, 7);
	shape.set::<i32>(&mut row, 1, 9);
	let frozen = row.freeze();
	let before = frozen.as_slice().to_vec();

	let mut thawed = frozen.thaw();
	assert_eq!(thawed.as_slice(), before.as_slice(), "thaw must not touch a single byte");

	thawed.set_timestamps(DateTime::from_millis(10), DateTime::from_millis(99));
	let refrozen = thawed.freeze();

	assert_eq!(refrozen.created_at(), DateTime::from_millis(10));
	assert_eq!(refrozen.updated_at(), DateTime::from_millis(99));
	assert_eq!(refrozen.time(), Some(DateTime::from_millis(333)), "an update must never re-stamp the event time");
	assert_eq!(refrozen.fingerprint(), shape.fingerprint());
	assert_eq!(shape.get::<i32>(refrozen.as_slice(), 0), 7);
	assert_eq!(shape.get::<i32>(refrozen.as_slice(), 1), 9);
	assert_eq!(refrozen.as_slice().len(), before.len(), "a header rewrite must not resize the row");
}

#[test]
fn thawing_one_handle_to_a_shared_row_must_not_mutate_the_other() {
	// The update path thaws a clone of bytes a reader still holds, so an in-place thaw would rewrite that reader.
	let shape = shape();
	let mut row = shape.allocate_table();
	row.set_timestamps(DateTime::from_millis(10), DateTime::from_millis(10));
	shape.set::<i32>(&mut row, 0, 7);
	let frozen = row.freeze();
	let alias = frozen.clone();
	let before = alias.as_slice().to_vec();

	let mut thawed = frozen.thaw();
	thawed.set_timestamps(DateTime::from_millis(10), DateTime::from_millis(99));
	shape.set::<i32>(&mut thawed, 0, 8);
	let refrozen = thawed.freeze();

	assert_eq!(alias.updated_at(), DateTime::from_millis(10), "the alias must still read its own stamp");
	assert_eq!(alias.as_slice(), before.as_slice(), "not one byte of the alias may move");
	assert_eq!(refrozen.updated_at(), DateTime::from_millis(99));
	assert_eq!(shape.get::<i32>(refrozen.as_slice(), 0), 8);
}

#[test]
#[should_panic(expected = "allocate_table on a shape of another family")]
fn a_shape_of_another_family_cannot_allocate_a_table_row() {
	// Series lays out byte for byte as table, so only this guard keeps the two fingerprints from being swapped.
	RowShape::new(RowFamily::Series, vec![RowShapeField::unconstrained("id", ValueType::Int4)]).allocate_table();
}
