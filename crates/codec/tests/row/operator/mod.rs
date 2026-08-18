// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::RowBuilder,
	operator::{EncodedOperatorRow, OPERATOR_HEADER_SIZE},
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::{
	factory::time::at_nanos,
	value::{Value, datetime::DateTime, value_type::ValueType},
};

fn timed_shape() -> RowShape {
	RowShape::new(RowFamily::Operator, vec![RowShapeField::unconstrained("v", ValueType::Int4)])
}

#[test]
fn the_header_is_eight_bytes_and_carries_nothing_but_a_time() {
	// A wider header would push every field offset and silently misread bodies written under this one.
	assert_eq!(OPERATOR_HEADER_SIZE, 8);
	assert_eq!(RowFamily::Operator.header_size(), OPERATOR_HEADER_SIZE);
}

#[test]
fn set_time_preserves_body() {
	// set_time must write exactly the header window, never the first byte of the body.
	let payload: Vec<u8> = vec![7, 8, 9, 10];
	let mut row = EncodedOperatorRow::new(&payload, at_nanos(7));
	assert_eq!(row.time(), at_nanos(7));

	row.set_time(at_nanos(99));
	assert_eq!(row.time(), at_nanos(99));
	assert_eq!(row.body(), payload.as_slice(), "the body must stay untouched");
}

#[test]
fn body_mut_windows_the_same_bytes_as_body() {
	// body_mut must window exactly body(), otherwise in-place writes land outside the payload.
	let payload: Vec<u8> = vec![1, 2, 3];
	let mut row = EncodedOperatorRow::new(&payload, DateTime::EPOCH);
	assert_eq!(row.body_mut(), payload.as_slice());
	assert_eq!(row.body(), payload.as_slice());
}

#[test]
fn a_row_round_trip_through_bytes_preserves_time() {
	// The store boundary must preserve the time header, otherwise windowing reads garbage.
	let row = EncodedOperatorRow::new(&[1, 2, 3], at_nanos(1234));
	let reloaded = EncodedOperatorRow::from(row.clone().into_bytes());

	assert_eq!(reloaded, row);
	assert_eq!(reloaded.time(), at_nanos(1234));
	assert_eq!(reloaded.body(), &[1, 2, 3]);
}

#[test]
fn timeless_rows_sort_above_every_cutoff() {
	// Absence is DateTime::MAX, which must outrank any cutoff a floor sweep can propose.
	let row = EncodedOperatorRow::timeless(&[]);
	assert_eq!(row.time(), DateTime::MAX);
	assert!(row.time() > at_nanos(u64::MAX - 1));
	assert!(row.body().is_empty());
}

#[test]
fn a_timeless_row_reads_back_as_absent_not_as_a_real_instant() {
	// Reading MAX as Some would hand windowing a far-future instant instead of "no time".
	assert_eq!(EncodedOperatorRow::timeless(&[1, 2]).row_time(), None);
	assert_eq!(EncodedOperatorRow::new(&[1, 2], at_nanos(5)).row_time(), Some(at_nanos(5)));
}

#[test]
fn byte_size_covers_header_and_body() {
	let row = EncodedOperatorRow::new(&[1, 2, 3, 4], DateTime::EPOCH);
	assert_eq!(row.byte_size().as_bytes(), row.len() as u64);
	assert_eq!(row.len(), OPERATOR_HEADER_SIZE + row.body().len());
}

#[test]
fn new_body_round_trips_exactly() {
	let payload: Vec<u8> = (0..=255).collect();
	let row = EncodedOperatorRow::new(&payload, at_nanos(7));
	assert_eq!(row.body(), payload.as_slice());
	assert_eq!(row.time(), at_nanos(7));
}

#[test]
fn a_freshly_allocated_operator_row_reads_as_timeless() {
	// Allocation must seed MAX, or an unset row reads as the epoch and lands in the wrong window.
	let shape = timed_shape();
	let encoded = shape.allocate_operator();

	assert_eq!(encoded.row_time(), None);
	assert_eq!(shape.time(&encoded), None);
}

#[test]
fn the_shape_lifts_a_time_written_through_the_builder() {
	// This is the carrier windowed operators read; if the shape cannot see it, every row is skipped.
	let shape = timed_shape();
	let mut encoded = shape.allocate_operator();
	shape.set_values(&mut encoded, &[Value::int4(42)]);
	encoded.set_time(at_nanos(60));

	let bytes = encoded.freeze_bytes();
	assert_eq!(shape.time(&bytes), Some(at_nanos(60)));
	assert_eq!(shape.get_value(&bytes, 0), Value::int4(42));
}

#[test]
fn an_operator_field_starts_after_the_time_header() {
	// A field overlapping the header would let set_values clobber the time it just wrote.
	let shape = timed_shape();
	assert!(shape.fields()[0].offset as usize >= OPERATOR_HEADER_SIZE);
}

#[test]
#[should_panic(expected = "operator rows carry no created_at")]
fn reading_created_at_off_an_operator_row_panics() {
	// The storage layout puts created_at at an offset this header does not have, so reading it is garbage.
	let shape = timed_shape();
	let encoded = shape.allocate_operator();
	let _ = shape.created_at(&encoded);
}

#[test]
#[should_panic(expected = "operator rows carry no updated_at")]
fn reading_updated_at_off_an_operator_row_panics() {
	let shape = timed_shape();
	let encoded = shape.allocate_operator();
	let _ = shape.updated_at(&encoded);
}

#[test]
#[should_panic(expected = "allocate_operator on a shape of another family")]
fn allocating_an_operator_row_from_a_pod_shape_panics() {
	let shape = RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("v", ValueType::Int4)]);
	let _ = shape.allocate_operator();
}
