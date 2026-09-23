// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	mem,
	panic::{AssertUnwindSafe, catch_unwind},
};

use arrow_buffer::{BooleanBuffer, NullBuffer};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	builder::ColumnBuilder,
	data::{Column, canonical::Canonical},
};
use reifydb_value::{
	util::bitmap,
	value::{Value, container::varlen_array::compact_parts, value_type::ValueType},
};

const ROWS: usize = 1000;

fn ints(n: usize) -> Vec<i64> {
	(0..n as i64).map(|i| i * 31 - 7).collect()
}

fn defined(n: usize) -> Vec<bool> {
	(0..n).map(|i| i % 5 != 2 && i % 7 != 0).collect()
}

fn strings(n: usize) -> Vec<String> {
	(0..n).map(|i| format!("s{i}-{}", "z".repeat(i % 6))).collect()
}

fn option_parts(buffer: &ColumnBuffer) -> (&ColumnBuffer, &BooleanBuffer) {
	match buffer.nulls() {
		Some(nulls) => (buffer, nulls.inner()),
		None => panic!("expected an option buffer, got {:?}", buffer.get_type()),
	}
}

fn utf8_bytes(buffer: &ColumnBuffer) -> &[u8] {
	match buffer {
		ColumnBuffer::Utf8 {
			container,
			..
		} => compact_parts(container).0,
		other => panic!("expected a utf8 buffer, got {:?}", other.get_type()),
	}
}

fn packed_bits_ptr(bits: &BooleanBuffer) -> *const u8 {
	match bitmap::packed_bytes(bits) {
		Cow::Borrowed(bytes) => bytes.as_ptr(),
		Cow::Owned(_) => panic!("a view at bit zero must borrow its packed bytes"),
	}
}

fn frozen_int8(n: usize) -> ColumnBuffer {
	let buffer = ColumnBuffer::int8(ints(n));
	buffer.into_builder().finish()
}

fn thaw_int8(buffer: &mut ColumnBuffer) -> *const i64 {
	let builder = mem::replace(buffer, ColumnBuffer::int8(Vec::<i64>::new())).into_builder();
	let ptr = match &builder {
		ColumnBuilder::Int8(values) => values.values_slice().as_ptr(),
		other => panic!("expected an int8 buffer, got {:?}", other.get_type()),
	};
	*buffer = builder.finish();
	ptr
}

fn frozen_option_int8(n: usize) -> ColumnBuffer {
	let buffer = ColumnBuffer::int8_with_bitvec(ints(n), defined(n));
	buffer.into_builder().finish()
}

#[test]
fn freeze_keeps_the_buffer_pointer() {
	// Freezing a column buffer must wrap its allocation, never copy it.
	let buffer = ColumnBuffer::int8(ints(ROWS));
	let before = buffer.as_slice::<i64>().as_ptr();
	let mut buffer = buffer.into_builder().finish();
	assert_eq!(buffer.as_slice::<i64>().as_ptr(), before);
	assert_eq!(buffer.as_slice::<i64>(), &ints(ROWS)[..]);
	assert_eq!(thaw_int8(&mut buffer), before, "a unique frozen buffer must thaw in place, never copy");
}

#[test]
fn clone_after_freeze_shares_the_buffer() {
	// A frozen buffer clone must alias the same rows, otherwise every scan batch copies the block.
	let mut buffer = frozen_int8(ROWS);
	let base = buffer.as_slice::<i64>().as_ptr();
	let copy = buffer.clone();
	assert_eq!(copy.as_slice::<i64>().as_ptr(), buffer.as_slice::<i64>().as_ptr());
	drop(copy);
	assert_eq!(thaw_int8(&mut buffer), base, "dropping the clone must release the allocation");
}

#[test]
fn slice_and_take_after_freeze_point_into_the_parent() {
	// A frozen slice must start exactly start rows into the parent and hold exactly its rows.
	let buffer = frozen_int8(ROWS);
	let base = buffer.as_slice::<i64>().as_ptr();
	let slice = buffer.slice(100, 350);
	assert_eq!(slice.len(), 250);
	assert_eq!(slice.as_slice::<i64>().as_ptr(), base.wrapping_add(100));
	assert_eq!(slice.as_slice::<i64>(), &ints(ROWS)[100..350]);
	let nested = slice.slice(50, 60);
	assert_eq!(nested.as_slice::<i64>().as_ptr(), base.wrapping_add(150));
	assert_eq!(nested.as_slice::<i64>(), &ints(ROWS)[150..160]);
	let head = buffer.take(10);
	assert_eq!(head.as_slice::<i64>().as_ptr(), base);
	assert_eq!(head.as_slice::<i64>(), &ints(ROWS)[..10]);
}

#[test]
fn push_on_a_shared_buffer_never_leaks_into_another_handle() {
	// A write through a clone or a head slice must copy first, never write rows another handle reads.
	let buffer = frozen_int8(ROWS);
	let mut copy = buffer.clone().into_builder();
	copy.push_value(Value::Int8(i64::MIN));
	assert_eq!(buffer.len(), ROWS);
	assert_eq!(buffer.as_slice::<i64>(), &ints(ROWS)[..]);
	let copy = copy.finish();
	assert_eq!(copy.get_value(ROWS), Value::Int8(i64::MIN));

	let mut head = buffer.slice(0, 10).into_builder();
	head.push_value(Value::Int8(i64::MAX));
	let head = head.finish();
	assert_eq!(head.as_slice::<i64>()[10], i64::MAX);
	assert_eq!(buffer.as_slice::<i64>()[10], ints(ROWS)[10], "a head slice push must never write the parent row");
}

#[test]
fn option_slice_shares_the_inner_rows_and_the_defined_bits() {
	// An option slice must share both the inner rows and the defined bits, and read none exactly where the parent
	// does.
	let buffer = frozen_option_int8(ROWS);
	let (inner, bits) = option_parts(&buffer);
	let base = inner.as_slice::<i64>().as_ptr();
	assert_eq!(bits.iter().collect::<Vec<_>>(), defined(ROWS));
	for (start, end) in [(0usize, 64usize), (13, 413), (8, 9), (999, 1000)] {
		let slice = buffer.slice(start, end);
		let (slice_inner, slice_bits) = option_parts(&slice);
		assert_eq!(slice_inner.as_slice::<i64>().as_ptr(), base.wrapping_add(start), "slice {start}..{end}");
		assert_eq!(slice_bits.iter().collect::<Vec<_>>(), &defined(ROWS)[start..end], "slice {start}..{end}");
		assert_eq!(
			slice_bits.inner().capacity(),
			bits.inner().capacity(),
			"slice {start}..{end} must share the parent defined bits"
		);
		for row in 0..slice.len() {
			assert_eq!(
				slice.get_value(row),
				buffer.get_value(start + row),
				"slice {start}..{end} row {row}"
			);
		}
		if start == 0 {
			assert_eq!(packed_bits_ptr(slice_bits), packed_bits_ptr(bits));
		}
	}
}

#[test]
fn option_slice_rejects_an_end_past_the_length() {
	// An option slice past the end must fail loudly, never return a short view.
	let buffer = frozen_option_int8(ROWS);
	let result = catch_unwind(AssertUnwindSafe(|| buffer.slice(10, ROWS + 1)));
	assert!(result.is_err());
}

#[test]
fn utf8_buffer_slice_references_the_parent_bytes() {
	// A frozen utf8 buffer slice must reference the parent bytes at exactly its first row's byte.
	let rows = strings(ROWS);
	let buffer = ColumnBuffer::utf8(rows.clone());
	let buffer = buffer.into_builder().finish();
	let base = utf8_bytes(&buffer).as_ptr();
	let start: usize = rows[..100].iter().map(|s| s.len()).sum();
	let slice = buffer.slice(100, 350);
	assert_eq!(utf8_bytes(&slice).as_ptr(), base.wrapping_add(start));
	for row in 0..slice.len() {
		assert_eq!(slice.get_value(row), Value::Utf8(rows[100 + row].clone()), "row {row}");
	}
	assert_eq!(utf8_bytes(&buffer.clone()).as_ptr(), base);
}

#[test]
fn canonical_from_buffer_freezes_without_copying() {
	// Lifting a buffer into a canonical must freeze it in place, so later clones and slices are zero copy.
	let buffer = ColumnBuffer::int8(ints(ROWS));
	let base = buffer.as_slice::<i64>().as_ptr();
	let canonical = Canonical::from_buffer(buffer);
	assert!(!canonical.nullable);
	assert!(canonical.buffer.nulls().is_none());
	assert_eq!(canonical.buffer.as_slice::<i64>().as_ptr(), base);
	let out = canonical.to_buffer();
	assert_eq!(out.as_slice::<i64>().as_ptr(), base);
	assert_eq!(out.as_slice::<i64>(), &ints(ROWS)[..]);
}

#[test]
fn canonical_from_option_buffer_lifts_the_defined_bits_to_nones() {
	// Canonical must share the buffer's validity without a copy and read none exactly on undefined rows.
	let buffer = frozen_option_int8(ROWS);
	let (inner, bits) = option_parts(&buffer);
	let base = inner.as_slice::<i64>().as_ptr();
	let bits_ptr = packed_bits_ptr(bits);
	let canonical = Canonical::from_column_buffer(&buffer).unwrap();
	assert!(canonical.nullable);
	assert_eq!(canonical.buffer.as_slice::<i64>().as_ptr(), base);
	let nones = canonical.buffer.nulls().expect("an option buffer must lift to a none bitmap");
	assert_eq!(nones.len(), ROWS);
	for (row, present) in defined(ROWS).into_iter().enumerate() {
		assert_eq!(nones.is_null(row), !present, "row {row}");
	}
	assert_eq!(nones.null_count(), defined(ROWS).iter().filter(|d| !**d).count());
	assert_eq!(packed_bits_ptr(nones.inner()), bits_ptr, "lifting must share the defined bits");

	let back = canonical.to_column_buffer().unwrap();
	let (back_inner, back_bits) = option_parts(&back);
	assert_eq!(back_inner.as_slice::<i64>().as_ptr(), base);
	assert_eq!(back_bits.iter().collect::<Vec<_>>(), defined(ROWS));
	for row in 0..ROWS {
		assert_eq!(back.get_value(row), buffer.get_value(row), "round trip row {row}");
	}
}

#[test]
fn column_from_column_buffer_and_slice_share_the_rows() {
	// A column and its slices must keep aliasing the rows of the buffer they were built from.
	let buffer = ColumnBuffer::int8(ints(ROWS));
	let base = buffer.as_slice::<i64>().as_ptr();
	let column = Column::from_canonical(Canonical::from_buffer(buffer));
	assert_eq!(column.len(), ROWS);
	assert_eq!(column.to_canonical().unwrap().buffer.as_slice::<i64>().as_ptr(), base);
	let slice = column.slice(100, 350).unwrap();
	assert_eq!(slice.len(), 250);
	let canonical = slice.to_canonical().unwrap();
	assert_eq!(canonical.buffer.as_slice::<i64>().as_ptr(), base.wrapping_add(100));
	let out = canonical.to_column_buffer().unwrap();
	assert_eq!(out.as_slice::<i64>().as_ptr(), base.wrapping_add(100));
	assert_eq!(out.as_slice::<i64>(), &ints(ROWS)[100..350]);
	for row in 0..slice.len() {
		assert_eq!(slice.data().get_value(row), Value::Int8(ints(ROWS)[100 + row]), "row {row}");
	}
}

#[test]
fn column_slice_of_an_option_column_keeps_nones_aligned() {
	// An optional column slice must read none on exactly the rows the parent reads none, at every offset.
	let column = Column::from_canonical(Canonical::from_buffer(ColumnBuffer::int8_with_bitvec(
		ints(ROWS),
		defined(ROWS),
	)));
	let base = column.to_canonical().unwrap().buffer.as_slice::<i64>().as_ptr();
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
		assert_eq!(canonical.buffer.as_slice::<i64>().as_ptr(), base.wrapping_add(start));
	}
}

#[test]
fn none_bitmap_filter_and_gather_match_the_model() {
	// Filtering and gathering must keep each row's none flag with that row, never shift it.
	let column = ColumnBuffer::int8_with_bitvec(ints(ROWS), defined(ROWS));
	let nones = column.nulls().expect("an optional column must carry a none bitmap");
	let keep: Vec<bool> = (0..ROWS).map(|i| i % 3 != 1).collect();
	let mut filtered = column.clone();
	filtered.filter(&BooleanBuffer::from(keep.clone())).unwrap();
	let filtered_nones = filtered.nulls().expect("a filtered optional column must keep its none bitmap");
	let expected: Vec<bool> = (0..ROWS).filter(|r| keep[*r]).map(|r| !defined(ROWS)[r]).collect();
	assert_eq!(filtered_nones.len(), expected.len());
	for (row, none) in expected.iter().enumerate() {
		assert_eq!(filtered_nones.is_null(row), *none, "filtered row {row}");
	}
	let mut kept_all = column.clone();
	kept_all.filter(&BooleanBuffer::new_set(ROWS)).unwrap();
	assert_eq!(kept_all.nulls(), Some(nones));
	let mut kept_none = column.clone();
	kept_none.filter(&BooleanBuffer::new_unset(ROWS)).unwrap();
	assert_eq!(kept_none.nulls().map(|n| n.len()), Some(0));

	let indices = [999usize, 0, 7, 7, 13, 500];
	let gathered = column.gather(&indices);
	let gathered_nones = gathered.nulls().expect("a gathered optional column must keep its none bitmap");
	for (row, index) in indices.iter().enumerate() {
		assert_eq!(gathered_nones.is_null(row), !defined(ROWS)[*index], "gathered row {row}");
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

fn option_int4(values: Vec<i32>, defined: Vec<bool>) -> ColumnBuffer {
	ColumnBuffer::int4(values).with_nulls(NullBuffer::new(BooleanBuffer::from(defined)))
}

#[track_caller]
fn assert_option_rows(buffer: &ColumnBuffer, values: &[i32], defined: &[bool], ctx: &str) {
	let (_, bits) = option_parts(buffer);
	assert_eq!(buffer.len(), defined.len(), "{ctx}: len");
	assert_eq!(bits.iter().collect::<Vec<_>>(), defined, "{ctx}: defined flags");
	assert_eq!(bits.count_set_bits(), defined.iter().filter(|d| **d).count(), "{ctx}: defined count");
	for (row, (value, present)) in values.iter().zip(defined).enumerate() {
		let expected = if *present {
			Value::Int4(*value)
		} else {
			Value::none_of(ValueType::Int4)
		};
		assert_eq!(buffer.get_value(row), expected, "{ctx}: row {row}");
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
			let view = parent.slice(offset, offset + len);
			let mut expected_bits = prefix_bits.clone();
			expected_bits.extend_from_slice(&bits[offset..offset + len]);
			let mut expected_values = prefix_values.clone();
			expected_values.extend_from_slice(&values[offset..offset + len]);

			let mut target = option_int4(prefix_values.clone(), prefix_bits.clone());
			target.extend(view.clone()).unwrap();
			assert_option_rows(&target, &expected_values, &expected_bits, &ctx);

			let mut builder = option_int4(prefix_values.clone(), prefix_bits.clone()).into_builder();
			builder.extend(view.clone()).unwrap();
			assert_option_rows(
				&builder.finish(),
				&expected_values,
				&expected_bits,
				&format!("{ctx} builder"),
			);

			let mut thawed = view.into_builder();
			thawed.push_value(Value::Int4(i32::MAX));
			thawed.push_none();
			let mut thawed_bits = bits[offset..offset + len].to_vec();
			thawed_bits.extend([true, false]);
			let mut thawed_values = values[offset..offset + len].to_vec();
			thawed_values.extend([i32::MAX, 0]);
			assert_option_rows(&thawed.finish(), &thawed_values, &thawed_bits, &format!("{ctx} thawed"));
		}
	}
}
