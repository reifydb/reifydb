// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, Int64Array};
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	builder::ColumnBuilder,
	data::{Column, ColumnData, canonical::Canonical},
	factory,
};
use reifydb_value::value::{Value, column_view::ColumnView, value_type::ValueType};

const ROWS: usize = 1000;

fn ints(n: usize) -> Vec<i64> {
	(0..n as i64).map(|i| i * 31 - 7).collect()
}

fn defined(n: usize) -> Vec<bool> {
	(0..n).map(|i| i % 5 != 2 && i % 7 != 0).collect()
}

fn int8_values(array: &ArrayRef) -> &[i64] {
	array.as_any().downcast_ref::<Int64Array>().expect("expected an int8 array").values()
}

fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
	ColumnView::try_from(column).unwrap()
}

#[test]
fn push_on_a_shared_buffer_never_leaks_into_another_handle() {
	// A builder from a clone or a head slice must copy first, never write rows another handle reads.
	let column = factory::int8("c", ints(ROWS));
	let shared = column.clone();
	let mut copy = ColumnBuilder::from_view(&view(&shared));
	copy.push_value(Value::Int8(i64::MIN));
	let copy = copy.finish("c");
	assert_eq!(column.1.len(), ROWS);
	assert_eq!(int8_values(&column.1), &ints(ROWS)[..]);
	assert_eq!(view(&copy).get_value(ROWS), Value::Int8(i64::MIN));

	let head = (column.0.clone(), column.1.slice(0, 10));
	let mut head = ColumnBuilder::from_view(&view(&head));
	head.push_value(Value::Int8(i64::MAX));
	let head = head.finish("c");
	assert_eq!(int8_values(&head.1)[10], i64::MAX);
	assert_eq!(int8_values(&column.1)[10], ints(ROWS)[10], "a head slice push must never write the parent row");
}

#[test]
fn canonical_from_column_shares_the_buffer_without_copying() {
	// Lifting a column into a canonical must keep its buffer, so later clones and slices are zero copy.
	let column = factory::int8("c", ints(ROWS));
	let base = int8_values(&column.1).as_ptr();
	let canonical = Canonical::from_column(&column).unwrap();
	assert!(!canonical.view().is_nullable());
	assert!(canonical.buffer().logical_nulls().is_none());
	assert_eq!(int8_values(canonical.buffer()).as_ptr(), base);
	let out = canonical.to_column("c");
	assert_eq!(int8_values(&out.1).as_ptr(), base);
	assert_eq!(out, column);
}

#[test]
fn canonical_from_option_column_lifts_the_defined_bits_to_nones() {
	// Canonical must share the column's validity without a copy and read none exactly on undefined rows.
	let column = factory::int8_with_bitvec("c", ints(ROWS), defined(ROWS));
	let base = int8_values(&column.1).as_ptr();
	let bits_ptr = column.1.nulls().expect("an optional column has a null buffer").validity().as_ptr();
	let canonical = Canonical::from_column(&column).unwrap();
	assert!(canonical.view().is_nullable());
	assert_eq!(canonical.field_type().value_type, Some(ValueType::Option(Box::new(ValueType::Int8))));
	assert_eq!(int8_values(canonical.buffer()).as_ptr(), base);
	let nones = canonical.nones().expect("an optional column must lift to a none bitmap");
	assert_eq!(nones.len(), ROWS);
	for (row, present) in defined(ROWS).into_iter().enumerate() {
		assert_eq!(nones.is_null(row), !present, "row {row}");
	}
	assert_eq!(nones.null_count(), defined(ROWS).iter().filter(|d| !**d).count());
	assert_eq!(nones.validity().as_ptr(), bits_ptr, "lifting must share the defined bits");

	let back = canonical.to_column("c");
	assert_eq!(int8_values(&back.1).as_ptr(), base);
	assert_eq!(back, column);
	for row in 0..ROWS {
		assert_eq!(view(&back).get_value(row), view(&column).get_value(row), "round trip row {row}");
	}
}

#[test]
fn column_from_canonical_and_slice_share_the_rows() {
	// A column and its slices must keep aliasing the rows of the column they were built from.
	let source = factory::int8("c", ints(ROWS));
	let base = int8_values(&source.1).as_ptr();
	let column = Column::from_canonical(Canonical::from_column(&source).unwrap());
	assert_eq!(column.len(), ROWS);
	assert_eq!(int8_values(column.to_canonical().unwrap().buffer()).as_ptr(), base);
	let slice = column.slice(100, 350).unwrap();
	assert_eq!(slice.len(), 250);
	let canonical = slice.to_canonical().unwrap();
	assert_eq!(int8_values(canonical.buffer()).as_ptr(), base.wrapping_add(100));
	let out = canonical.to_column("c");
	assert_eq!(int8_values(&out.1).as_ptr(), base.wrapping_add(100));
	assert_eq!(int8_values(&out.1), &ints(ROWS)[100..350]);
	for row in 0..slice.len() {
		assert_eq!(slice.data().get_value(row), Value::Int8(ints(ROWS)[100 + row]), "row {row}");
	}
}

#[test]
fn column_slice_of_an_option_column_keeps_nones_aligned() {
	// An optional column slice must read none on exactly the rows the parent reads none, at every offset.
	let source = factory::int8_with_bitvec("c", ints(ROWS), defined(ROWS));
	let column = Column::from_canonical(Canonical::from_column(&source).unwrap());
	let base = int8_values(column.to_canonical().unwrap().buffer()).as_ptr();
	for (start, end) in [(0usize, 100usize), (13, 413), (63, 65), (500, 1000)] {
		let slice = column.slice(start, end).unwrap();
		let nones = slice.nones().expect("an optional slice must keep its none bitmap");
		assert_eq!(nones.len(), end - start);
		for row in 0..slice.len() {
			assert_eq!(nones.is_null(row), !defined(ROWS)[start + row], "slice {start}..{end} row {row}");
			assert_eq!(
				slice.data().get_value(row),
				column.data().get_value(start + row),
				"slice {start}..{end} row {row}"
			);
		}
		let canonical = slice.to_canonical().unwrap();
		assert_eq!(int8_values(canonical.buffer()).as_ptr(), base.wrapping_add(start));
	}
}

const VIEW_OFFSETS: [usize; 8] = [0, 1, 7, 8, 9, 63, 64, 65];
const VIEW_LENGTHS: [usize; 8] = [0, 1, 7, 8, 9, 64, 65, 200];
const VIEW_PARENT_ROWS: usize = 300;

fn pattern(len: usize, seed: u64) -> Vec<bool> {
	let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
	(0..len).map(|_| {
		state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		(state >> 33) & 1 == 1
	})
	.collect()
}

fn option_int4(values: Vec<i32>, defined: Vec<bool>) -> (FieldRef, ArrayRef) {
	factory::int4_with_bitvec("c", values, defined)
}

#[track_caller]
fn assert_option_rows(column: &(FieldRef, ArrayRef), values: &[i32], defined: &[bool], ctx: &str) {
	let view = view(column);
	assert_eq!(view.len(), defined.len(), "{ctx}: len");
	assert_eq!(
		(0..view.len()).map(|row| view.is_defined(row)).collect::<Vec<_>>(),
		defined,
		"{ctx}: defined flags"
	);
	assert_eq!(view.none_count(), defined.iter().filter(|d| !**d).count(), "{ctx}: none count");
	for (row, (value, present)) in values.iter().zip(defined).enumerate() {
		let expected = if *present {
			Value::Int4(*value)
		} else {
			Value::none_of(ValueType::Int4)
		};
		assert_eq!(view.get_value(row), expected, "{ctx}: row {row}");
	}
}

#[test]
fn extend_from_a_view_reads_at_its_offset() {
	// Extending from an offset slice must copy the slice's defined bits, never the bits at the start of its parent.
	let bits = pattern(VIEW_PARENT_ROWS, 14);
	let values: Vec<i32> = (0..VIEW_PARENT_ROWS as i32).map(|i| i * 31 - 7).collect();
	let parent = option_int4(values.clone(), bits.clone());
	let prefix_bits = pattern(11, 15);
	let prefix_values: Vec<i32> = (0..11).map(|i| -1000 - i).collect();
	for offset in VIEW_OFFSETS {
		for len in VIEW_LENGTHS {
			let ctx = format!("view {offset}+{len}");
			let slice = (parent.0.clone(), parent.1.slice(offset, len));
			let mut expected_bits = prefix_bits.clone();
			expected_bits.extend_from_slice(&bits[offset..offset + len]);
			let mut expected_values = prefix_values.clone();
			expected_values.extend_from_slice(&values[offset..offset + len]);

			let target = option_int4(prefix_values.clone(), prefix_bits.clone());
			let mut builder = ColumnBuilder::from_view(&view(&target));
			builder.extend(&view(&slice)).unwrap();
			assert_option_rows(
				&builder.finish("c"),
				&expected_values,
				&expected_bits,
				&format!("{ctx} builder"),
			);

			let mut thawed = ColumnBuilder::from_view(&view(&slice));
			thawed.push_value(Value::Int4(i32::MAX));
			thawed.push_none();
			let mut thawed_bits = bits[offset..offset + len].to_vec();
			thawed_bits.extend([true, false]);
			let mut thawed_values = values[offset..offset + len].to_vec();
			thawed_values.extend([i32::MAX, 0]);
			assert_option_rows(&thawed.finish("c"), &thawed_values, &thawed_bits, &format!("{ctx} thawed"));
		}
	}
}
