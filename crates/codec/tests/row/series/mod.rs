// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{RowBuilder, SHAPE_HEADER_SIZE},
	series::EncodedSeriesRow,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::value::{datetime::DateTime, value_type::ValueType};

fn shape() -> RowShape {
	RowShape::new(
		RowFamily::Series,
		vec![
			RowShapeField::unconstrained("key", ValueType::Int8),
			RowShapeField::unconstrained("value", ValueType::Int4),
		],
	)
}

#[test]
fn the_series_header_is_the_full_source_header() {
	// The queue family sits 8 bytes wider, so borrowing its constant shifts every field offset.
	assert_eq!(shape().header_size(), SHAPE_HEADER_SIZE);
	assert_eq!(shape().header_size(), 33);
}

#[test]
fn the_typed_view_reads_the_stamps_the_builder_wrote() {
	// The stamps are adjacent 8-byte slots, so swapped offsets round trip undetected unless the values differ.
	let shape = shape();
	let mut row = shape.allocate_series();
	row.set_timestamps(DateTime::from_millis(111), DateTime::from_millis(222));
	row.set_time(DateTime::from_millis(333));

	let frozen = row.freeze_bytes();
	let typed = EncodedSeriesRow::view(&frozen);

	assert_eq!(typed.created_at(), DateTime::from_millis(111));
	assert_eq!(typed.updated_at(), DateTime::from_millis(222));
	assert_eq!(typed.time(), Some(DateTime::from_millis(333)));
	assert_eq!(typed.fingerprint(), shape.fingerprint());
}

#[test]
fn the_key_in_field_zero_and_the_same_instant_in_time_stay_independent() {
	// A series writes its key into field 0 and again into #time, which must never share a slot.
	let shape = shape();
	let mut row = shape.allocate_series();
	shape.set::<i64>(&mut row, 0, 1_700_000_000_000);
	row.set_time(DateTime::from_millis(1_700_000_000_000));

	let frozen = row.freeze_bytes();

	assert_eq!(shape.get::<i64>(&frozen, 0), 1_700_000_000_000);
	assert_eq!(EncodedSeriesRow::view(&frozen).time(), Some(DateTime::from_millis(1_700_000_000_000)));
}

#[test]
fn the_typed_definedness_agrees_with_the_shape() {
	// A header width disagreeing with the shape's reads a field byte as a validity bit.
	let shape = shape();
	let mut row = shape.allocate_series();
	shape.set::<i32>(&mut row, 1, 9);

	let frozen = row.freeze_bytes();
	let typed = EncodedSeriesRow::view(&frozen);

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
	let mut row = shape.allocate_series();
	shape.set::<i64>(&mut row, 0, 7);

	let frozen = row.freeze_bytes();
	let typed = EncodedSeriesRow::view(&frozen);

	assert_eq!(typed.body().len(), frozen.len() - SHAPE_HEADER_SIZE);
	assert_eq!(typed.body(), &frozen.as_slice()[shape.header_size()..]);
}

#[test]
fn thawing_a_row_refreshes_updated_at_without_re_stamping_the_event_instant() {
	// The event time is the series key, so an update that re-stamped or unflagged it would relocate the row.
	let shape = shape();
	let mut row = shape.allocate_series();
	row.set_timestamps(DateTime::from_millis(1), DateTime::from_millis(1));
	row.set_time(DateTime::from_millis(1_700_000_000_000));
	shape.set::<i64>(&mut row, 0, 1_700_000_000_000);
	shape.set::<i32>(&mut row, 1, 42);
	let frozen = row.freeze();

	let mut thawed = frozen.thaw();
	assert_eq!(thawed.time(), Some(DateTime::from_millis(1_700_000_000_000)), "the flag bit must survive a thaw");
	thawed.set_timestamps(DateTime::from_millis(1), DateTime::from_millis(2));
	shape.set::<i32>(&mut thawed, 1, 43);
	let refrozen = thawed.freeze();

	assert_eq!(refrozen.time(), Some(DateTime::from_millis(1_700_000_000_000)));
	assert_eq!(refrozen.created_at(), DateTime::from_millis(1));
	assert_eq!(refrozen.updated_at(), DateTime::from_millis(2));
	assert_eq!(shape.get::<i64>(refrozen.as_slice(), 0), 1_700_000_000_000, "the key column must not move");
	assert_eq!(shape.get::<i32>(refrozen.as_slice(), 1), 43);
}

#[test]
#[should_panic(expected = "allocate_series on a shape of another family")]
fn a_shape_of_another_family_cannot_allocate_a_series_row() {
	// A ring buffer shape is the same width and the same layout, so no size check could ever catch this.
	RowShape::new(RowFamily::RingBuffer, vec![RowShapeField::unconstrained("key", ValueType::Int8)])
		.allocate_series();
}
