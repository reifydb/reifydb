// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	panic::{AssertUnwindSafe, catch_unwind},
};

use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	data::{Column, canonical::Canonical},
	mask::RowMask,
	nones::NoneBitmap,
};
use reifydb_value::{util::bitvec::BitVec, value::Value};

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

fn option_parts(buffer: &ColumnBuffer) -> (&ColumnBuffer, &BitVec) {
	match buffer {
		ColumnBuffer::Option {
			inner,
			bitvec,
		} => (inner, bitvec),
		other => panic!("expected an option buffer, got {:?}", other.get_type()),
	}
}

fn utf8_bytes(buffer: &ColumnBuffer) -> &[u8] {
	match buffer {
		ColumnBuffer::Utf8 {
			container,
			..
		} => container.inner().compact_parts().0,
		other => panic!("expected a utf8 buffer, got {:?}", other.get_type()),
	}
}

fn packed_ptr(bits: &BitVec) -> *const u8 {
	match bits.to_packed_bytes() {
		Cow::Borrowed(bytes) => bytes.as_ptr(),
		Cow::Owned(_) => panic!("a view at bit zero must borrow its packed bytes"),
	}
}

fn frozen_int8(n: usize) -> ColumnBuffer {
	let mut buffer = ColumnBuffer::int8(ints(n));
	buffer.freeze();
	buffer
}

fn thaw_int8(buffer: &mut ColumnBuffer) -> *const i64 {
	match buffer {
		ColumnBuffer::Int8(container) => container.data_mut().as_ptr(),
		other => panic!("expected an int8 buffer, got {:?}", other.get_type()),
	}
}

fn frozen_option_int8(n: usize) -> ColumnBuffer {
	let mut buffer = ColumnBuffer::int8_with_bitvec(ints(n), defined(n));
	buffer.freeze();
	buffer
}

#[test]
fn freeze_keeps_the_buffer_pointer() {
	// Freezing a column buffer must wrap its allocation, never copy it.
	let mut buffer = ColumnBuffer::int8(ints(ROWS));
	let before = buffer.as_slice::<i64>().as_ptr();
	buffer.freeze();
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
	let mut copy = buffer.clone();
	copy.push_value(Value::Int8(i64::MIN));
	assert_eq!(buffer.len(), ROWS);
	assert_eq!(buffer.as_slice::<i64>(), &ints(ROWS)[..]);
	assert_eq!(copy.get_value(ROWS), Value::Int8(i64::MIN));

	let mut head = buffer.slice(0, 10);
	head.push_value(Value::Int8(i64::MAX));
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
	assert_eq!(bits.to_vec(), defined(ROWS));
	for (start, end) in [(0usize, 64usize), (13, 413), (8, 9), (999, 1000)] {
		let slice = buffer.slice(start, end);
		let (slice_inner, slice_bits) = option_parts(&slice);
		assert_eq!(slice_inner.as_slice::<i64>().as_ptr(), base.wrapping_add(start), "slice {start}..{end}");
		assert_eq!(slice_bits.to_vec(), &defined(ROWS)[start..end], "slice {start}..{end}");
		assert_eq!(
			slice_bits.capacity(),
			bits.capacity(),
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
			assert_eq!(packed_ptr(slice_bits), packed_ptr(bits));
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
	let mut buffer = ColumnBuffer::utf8(rows.clone());
	buffer.freeze();
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
	assert!(canonical.nones.is_none());
	assert_eq!(canonical.buffer.as_slice::<i64>().as_ptr(), base);
	let out = canonical.to_buffer();
	assert_eq!(out.as_slice::<i64>().as_ptr(), base);
	assert_eq!(out.as_slice::<i64>(), &ints(ROWS)[..]);
}

#[test]
fn canonical_from_option_buffer_lifts_the_defined_bits_to_nones() {
	// The option bitvec marks defined rows, so the lifted none bitmap must report exactly the opposite rows as
	// none.
	let buffer = frozen_option_int8(ROWS);
	let (inner, bits) = option_parts(&buffer);
	let base = inner.as_slice::<i64>().as_ptr();
	let bits_ptr = packed_ptr(bits);
	let canonical = Canonical::from_column_buffer(&buffer).unwrap();
	assert!(canonical.nullable);
	assert!(
		!matches!(canonical.buffer, ColumnBuffer::Option { .. }),
		"nullability must be lifted out of the buffer"
	);
	assert_eq!(canonical.buffer.as_slice::<i64>().as_ptr(), base);
	let nones = canonical.nones.as_ref().expect("an option buffer must lift to a none bitmap");
	assert_eq!(nones.len(), ROWS);
	for (row, present) in defined(ROWS).into_iter().enumerate() {
		assert_eq!(nones.is_none(row), !present, "row {row}");
	}
	assert_eq!(nones.none_count(), defined(ROWS).iter().filter(|d| !**d).count());
	assert_eq!(packed_ptr(&nones.to_defined_bitvec()), bits_ptr, "lifting must share the defined bits");

	let back = canonical.to_column_buffer().unwrap();
	let (back_inner, back_bits) = option_parts(&back);
	assert_eq!(back_inner.as_slice::<i64>().as_ptr(), base);
	assert_eq!(back_bits.to_vec(), defined(ROWS));
	for row in 0..ROWS {
		assert_eq!(back.get_value(row), buffer.get_value(row), "round trip row {row}");
	}
}

#[test]
fn column_from_column_buffer_and_slice_share_the_rows() {
	// A column and its slices must keep aliasing the rows of the buffer they were built from.
	let buffer = ColumnBuffer::int8(ints(ROWS));
	let base = buffer.as_slice::<i64>().as_ptr();
	let column = Column::from_column_buffer(buffer);
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
		assert_eq!(slice.get_value(row), Value::Int8(ints(ROWS)[100 + row]), "row {row}");
	}
}

#[test]
fn column_slice_of_an_option_column_keeps_nones_aligned() {
	// An optional column slice must read none on exactly the rows the parent reads none, at every offset.
	let column = Column::from_column_buffer(ColumnBuffer::int8_with_bitvec(ints(ROWS), defined(ROWS)));
	let base = column.to_canonical().unwrap().buffer.as_slice::<i64>().as_ptr();
	for (start, end) in [(0usize, 100usize), (13, 413), (63, 65), (500, 1000)] {
		let slice = column.slice(start, end).unwrap();
		let nones = slice.nones().expect("an optional slice must keep its none bitmap");
		assert_eq!(nones.len(), end - start);
		for row in 0..slice.len() {
			assert_eq!(nones.is_none(row), !defined(ROWS)[start + row], "slice {start}..{end} row {row}");
			assert_eq!(
				slice.get_value(row),
				column.get_value(start + row),
				"slice {start}..{end} row {row}"
			);
		}
		let canonical = slice.to_canonical().unwrap();
		assert_eq!(canonical.buffer.as_slice::<i64>().as_ptr(), base.wrapping_add(start));
	}
}

#[test]
fn none_bitmap_slices_and_writes_stay_isolated() {
	// A none bitmap slice must read exactly its rows, and set_none on one handle must never mark a row none in
	// another.
	let bits = BitVec::from(defined(ROWS));
	let nones = NoneBitmap::from_defined_bitvec(&bits);
	assert_eq!(packed_ptr(&nones.to_defined_bitvec()), packed_ptr(&bits));
	let slice = nones.slice(13, 413);
	for row in 0..slice.len() {
		assert_eq!(slice.is_none(row), !defined(ROWS)[13 + row], "row {row}");
	}
	assert_eq!(slice.none_count(), defined(ROWS)[13..413].iter().filter(|d| !**d).count());

	let present_row = (0..ROWS).find(|r| defined(ROWS)[*r]).unwrap();
	let mut copy = nones.clone();
	copy.set_none(present_row);
	assert!(copy.is_none(present_row));
	assert!(!nones.is_none(present_row), "set_none must never leak into the original bitmap");
	assert!(bits.get(present_row), "set_none must never leak into the source bitvec");
	assert_eq!(copy.none_count(), nones.none_count() + 1);

	let mut sliced_copy = nones.slice(present_row, ROWS);
	sliced_copy.set_none(0);
	assert!(!nones.is_none(present_row), "set_none on a slice must never leak into the parent");

	assert_eq!(NoneBitmap::all_present(70).none_count(), 0);
	assert_eq!(NoneBitmap::all_none(70).none_count(), 70);
	assert!(NoneBitmap::all_none(70).is_none(69));
	assert!(!NoneBitmap::all_present(70).is_none(69));
}

#[test]
fn none_bitmap_filter_and_gather_match_the_model() {
	// Filtering and gathering must keep each row's none flag with that row, never shift it.
	let nones = NoneBitmap::from_defined_bitvec(&BitVec::from(defined(ROWS)));
	let keep: Vec<bool> = (0..ROWS).map(|i| i % 3 != 1).collect();
	let mut mask = RowMask::none_set(ROWS);
	for (row, k) in keep.iter().enumerate() {
		mask.set(row, *k);
	}
	let filtered = nones.filter(&mask);
	let expected: Vec<bool> = (0..ROWS).filter(|r| keep[*r]).map(|r| !defined(ROWS)[r]).collect();
	assert_eq!(filtered.len(), expected.len());
	for (row, none) in expected.iter().enumerate() {
		assert_eq!(filtered.is_none(row), *none, "filtered row {row}");
	}
	assert_eq!(nones.filter(&RowMask::all_set(ROWS)), nones);
	assert_eq!(nones.filter(&RowMask::none_set(ROWS)).len(), 0);

	let indices = [999usize, 0, 7, 7, 13, 500];
	let gathered = nones.gather(&indices);
	for (row, index) in indices.iter().enumerate() {
		assert_eq!(gathered.is_none(row), !defined(ROWS)[*index], "gathered row {row}");
	}
}

fn mask_from(bits: &[bool]) -> RowMask {
	let mut mask = RowMask::none_set(bits.len());
	for (row, bit) in bits.iter().enumerate() {
		mask.set(row, *bit);
	}
	mask
}

#[test]
fn row_mask_operations_match_the_model() {
	// Every mask operation must agree bit for bit with a plain bool model, including on offset slices.
	let a: Vec<bool> = (0..ROWS).map(|i| i % 3 == 0).collect();
	let b: Vec<bool> = (0..ROWS).map(|i| i % 4 == 1 || i % 7 == 0).collect();
	let ma = mask_from(&a);
	let mb = mask_from(&b);
	assert_eq!(ma.len(), ROWS);
	assert_eq!(ma.popcount(), a.iter().filter(|x| **x).count());
	let and: Vec<bool> = a.iter().zip(&b).map(|(x, y)| *x && *y).collect();
	let or: Vec<bool> = a.iter().zip(&b).map(|(x, y)| *x || *y).collect();
	let not: Vec<bool> = a.iter().map(|x| !*x).collect();
	assert_eq!(ma.and(&mb).as_bitvec().to_vec(), and);
	assert_eq!(ma.or(&mb).as_bitvec().to_vec(), or);
	assert_eq!(ma.not().as_bitvec().to_vec(), not);
	assert_eq!(ma.not().popcount(), ROWS - ma.popcount());

	let sa = ma.slice(13, 413);
	let sb = mb.slice(200, 600);
	for row in 0..sa.len() {
		assert_eq!(sa.get(row), a[13 + row], "slice row {row}");
	}
	let sliced_and: Vec<bool> = (0..400).map(|r| a[13 + r] && b[200 + r]).collect();
	assert_eq!(sa.and(&sb).as_bitvec().to_vec(), sliced_and, "and across different offsets");
	let sliced_or: Vec<bool> = (0..400).map(|r| a[13 + r] || b[200 + r]).collect();
	assert_eq!(sa.or(&sb).as_bitvec().to_vec(), sliced_or, "or across different offsets");
	assert_eq!(sa.not().as_bitvec().to_vec(), (0..400).map(|r| !a[13 + r]).collect::<Vec<_>>());

	let parts = [ma.slice(0, 13), ma.slice(13, 413), ma.slice(413, ROWS)];
	assert_eq!(RowMask::concat(&parts), ma, "concat of adjacent slices must rebuild exactly the original");

	let mut written = ma.slice(0, 100);
	written.set(5, !a[5]);
	assert_eq!(ma.get(5), a[5], "a write through a slice must never leak into the parent mask");
	assert_eq!(written.get(5), !a[5]);
}

#[test]
fn row_mask_rejects_mismatched_lengths() {
	// Combining masks of different lengths must fail loudly, never silently truncate.
	let a = RowMask::all_set(10);
	let b = RowMask::all_set(11);
	assert!(catch_unwind(AssertUnwindSafe(|| a.and(&b))).is_err());
	assert!(catch_unwind(AssertUnwindSafe(|| a.or(&b))).is_err());
	assert!(catch_unwind(AssertUnwindSafe(|| a.slice(3, 11))).is_err());
}
