// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use arrow_array::{Array, BooleanArray, Decimal128Array, PrimitiveArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer, ScalarBuffer};
use reifydb_value::{
	util::{bitmap, kernel},
	value::{
		Value,
		container::{
			decimal_array::{int16_array, u128s, uint16_array},
			temporal_array::{
				date_array, dates, datetime_array, datetimes, duration_array, durations, time_array,
				times,
			},
			uuid_array::{uuid4_array, uuid4s, uuid7_array, uuid7s},
		},
		date::Date,
		datetime::DateTime,
		duration::Duration,
		is::{IsNumber, IsTemporal, IsUuid},
		time::Time,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::value::column::{
	ColumnBuffer,
	buffer::take::{as_array, default_row, wrap_array},
	builder::ColumnBuilder,
};

impl ColumnBuffer {
	pub fn merge_rows(&self, other: &ColumnBuffer, mask: &BooleanBuffer, len: usize) -> ColumnBuffer {
		if !alignable(self, other, len) || mask.len() < len {
			return merge_rows_by_value(self, other, mask, len);
		}

		let old = normalized(self, len);
		let new = normalized(other, len);
		let picker = BooleanArray::new(bitmap::resize(mask, len), None);
		let merged = kernel::merged(&picker, as_array(&new), as_array(&old));
		let valid = BooleanBuffer::collect_bool(len, |row| match picker.value(row) {
			true => !new.none_at(row),
			false => !old.none_at(row),
		});
		finish_merge(self, merged.as_ref(), valid)
	}

	pub fn scatter_merge(
		&self,
		other: &ColumnBuffer,
		then_mask: &BooleanBuffer,
		else_mask: &BooleanBuffer,
		total_len: usize,
	) -> ColumnBuffer {
		match (self.nulls(), other.nulls()) {
			(Some(a_nulls), Some(b_nulls)) => {
				let (a_inner, _) = self.clone().split_nulls();
				let (b_inner, _) = other.clone().split_nulls();
				let merged_inner = a_inner.scatter_merge(&b_inner, then_mask, else_mask, total_len);
				let merged = merge_validity_bitvecs(
					a_nulls.inner(),
					b_nulls.inner(),
					then_mask,
					else_mask,
					total_len,
				);
				return merged_inner.with_nulls(NullBuffer::new(merged));
			}
			(None, None) => {
				if let Some(result) = scatter_merge_typed(self, other, then_mask, else_mask, total_len)
				{
					return result;
				}
			}
			_ => {}
		}

		scatter_merge_generic(self, other, then_mask, else_mask, total_len)
	}
}

fn merge_validity_bitvecs(
	then_bv: &BooleanBuffer,
	else_bv: &BooleanBuffer,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> BooleanBuffer {
	BooleanBuffer::collect_bool(total_len, |i| {
		if then_mask.value(i) {
			i < then_bv.len() && then_bv.value(i)
		} else if else_mask.value(i) {
			i < else_bv.len() && else_bv.value(i)
		} else {
			false
		}
	})
}

fn scatter_merge_generic(
	self_col: &ColumnBuffer,
	other: &ColumnBuffer,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> ColumnBuffer {
	if !alignable(self_col, other, total_len) || then_mask.len() < total_len || else_mask.len() < total_len {
		return scatter_merge_by_value(self_col, other, then_mask, else_mask, total_len);
	}

	let then_side = normalized(self_col, total_len);
	let else_side = normalized(other, total_len);
	let filler = default_row(&then_side);
	let pairs: Vec<(usize, usize)> = (0..total_len)
		.map(|row| match (then_mask.value(row), else_mask.value(row)) {
			(true, _) => (0, row),
			(false, true) => (1, row),
			(false, false) => (2, 0),
		})
		.collect();
	let merged = kernel::picked(&[as_array(&then_side), as_array(&else_side), as_array(&filler)], &pairs);
	let valid = BooleanBuffer::collect_bool(total_len, |row| match (then_mask.value(row), else_mask.value(row)) {
		(true, _) => !then_side.none_at(row),
		(false, true) => !else_side.none_at(row),
		(false, false) => false,
	});
	finish_merge(self_col, merged.as_ref(), valid)
}

fn scatter_merge_by_value(
	self_col: &ColumnBuffer,
	other: &ColumnBuffer,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> ColumnBuffer {
	let result_type = self_col.get_type();
	let mut builder = ColumnBuilder::with_capacity(result_type.clone(), total_len);
	for i in 0..total_len {
		if then_mask.value(i) {
			builder.push_value(self_col.get_value(i));
		} else if else_mask.value(i) {
			builder.push_value(other.get_value(i));
		} else {
			builder.push_value(Value::none_of(result_type.clone()));
		}
	}
	builder.finish()
}

fn merge_rows_by_value(old: &ColumnBuffer, new: &ColumnBuffer, mask: &BooleanBuffer, len: usize) -> ColumnBuffer {
	let mut builder = ColumnBuilder::with_capacity(old.get_type(), len);
	for row in 0..len {
		match mask.value(row) {
			true => builder.push_value(new.get_value(row)),
			false => builder.push_value(old.get_value(row)),
		}
	}
	builder.finish()
}

fn alignable(old: &ColumnBuffer, new: &ColumnBuffer, len: usize) -> bool {
	old.len() == len && new.len() == len && as_array(old).data_type() == as_array(new).data_type()
}

fn normalized(source: &ColumnBuffer, len: usize) -> ColumnBuffer {
	match source.nulls().is_some_and(|nulls| nulls.null_count() > 0) {
		true => source.extract_rows(&(0..len).collect::<Vec<_>>()),
		false => source.clone(),
	}
}

fn finish_merge(source: &ColumnBuffer, merged: &dyn Array, valid: BooleanBuffer) -> ColumnBuffer {
	let shell = ColumnBuilder::with_capacity(source.get_type(), 0).finish();
	let result = wrap_array(&shell, merged);
	match source.nulls().is_some() || valid.count_set_bits() != valid.len() {
		true => result.replace_nulls(Some(NullBuffer::new(valid))),
		false => result.replace_nulls(None),
	}
}

fn scatter_merge_typed(
	self_col: &ColumnBuffer,
	other: &ColumnBuffer,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> Option<ColumnBuffer> {
	macro_rules! native_kernel {
		($variant:ident, $t:ty) => {
			if let (ColumnBuffer::$variant(a), ColumnBuffer::$variant(b)) = (self_col, other) {
				let (data, validity) =
					number_scatter::<$t>(a.values(), b.values(), then_mask, else_mask, total_len);
				let inner = ColumnBuffer::$variant(PrimitiveArray::new(ScalarBuffer::from(data), None));
				return Some(finalize(inner, validity));
			}
		};
	}
	macro_rules! number_kernel {
		($variant:ident, $t:ty, $values:path, $build:ident) => {
			if let (ColumnBuffer::$variant(a), ColumnBuffer::$variant(b)) = (self_col, other) {
				let (data, validity) =
					number_scatter::<$t>(&$values(a), &$values(b), then_mask, else_mask, total_len);
				let inner = ColumnBuffer::$variant($build(data));
				return Some(finalize(inner, validity));
			}
		};
	}
	macro_rules! temporal_kernel {
		($variant:ident, $t:ty, $typed:ident, $build:ident) => {
			if let (ColumnBuffer::$variant(a), ColumnBuffer::$variant(b)) = (self_col, other) {
				let (data, validity) =
					temporal_scatter::<$t>($typed(a), $typed(b), then_mask, else_mask, total_len);
				let inner = ColumnBuffer::$variant($build(data));
				return Some(finalize(inner, validity));
			}
		};
	}
	macro_rules! uuid_kernel {
		($variant:ident, $t:ty, $typed:ident, $build:ident) => {
			if let (ColumnBuffer::$variant(a), ColumnBuffer::$variant(b)) = (self_col, other) {
				let (data, validity) =
					uuid_scatter::<$t>($typed(a), $typed(b), then_mask, else_mask, total_len);
				let inner = ColumnBuffer::$variant($build(data));
				return Some(finalize(inner, validity));
			}
		};
	}

	if let (ColumnBuffer::Bool(a), ColumnBuffer::Bool(b)) = (self_col, other) {
		let (data, validity) = bool_scatter(a, b, then_mask, else_mask, total_len);
		let inner = ColumnBuffer::Bool(BooleanArray::from(data));
		return Some(finalize(inner, validity));
	}

	native_kernel!(Float4, f32);
	native_kernel!(Float8, f64);
	native_kernel!(Int1, i8);
	native_kernel!(Int2, i16);
	native_kernel!(Int4, i32);
	native_kernel!(Int8, i64);
	number_kernel!(Int16, i128, Decimal128Array::values, int16_array);
	native_kernel!(Uint1, u8);
	native_kernel!(Uint2, u16);
	native_kernel!(Uint4, u32);
	native_kernel!(Uint8, u64);
	number_kernel!(Uint16, u128, u128s, uint16_array);

	temporal_kernel!(Date, Date, dates, date_array);
	temporal_kernel!(DateTime, DateTime, datetimes, datetime_array);
	temporal_kernel!(Time, Time, times, time_array);
	temporal_kernel!(Duration, Duration, durations, duration_array);

	uuid_kernel!(Uuid4, Uuid4, uuid4s, uuid4_array);
	uuid_kernel!(Uuid7, Uuid7, uuid7s, uuid7_array);

	None
}

fn finalize(inner: ColumnBuffer, validity: Option<BooleanBuffer>) -> ColumnBuffer {
	match validity {
		Some(bv) => inner.with_nulls(NullBuffer::new(bv)),
		None => inner,
	}
}

fn bool_scatter(
	a: &BooleanArray,
	b: &BooleanArray,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> (BooleanBuffer, Option<BooleanBuffer>) {
	let a_data = a.values();
	let b_data = b.values();
	let mut out = BooleanBufferBuilder::new(total_len);
	let mut validity: Option<BooleanBufferBuilder> = None;
	for i in 0..total_len {
		let in_then = then_mask.value(i);
		let in_else = !in_then && else_mask.value(i);
		let bit = if in_then && i < a_data.len() {
			a_data.value(i)
		} else if in_else && i < b_data.len() {
			b_data.value(i)
		} else {
			false
		};
		out.append(bit);
		if !in_then && !in_else {
			let v = validity.get_or_insert_with(|| {
				let mut bv = BooleanBufferBuilder::new(total_len);
				bv.append_n(i, true);
				bv
			});
			v.append(false);
		} else if let Some(v) = validity.as_mut() {
			v.append(true);
		}
	}
	(out.finish(), validity.map(|mut v| v.finish()))
}

fn number_scatter<T>(
	a_data: &[T],
	b_data: &[T],
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> (Vec<T>, Option<BooleanBuffer>)
where
	T: IsNumber + Clone + Default + Debug,
{
	let mut out: Vec<T> = Vec::with_capacity(total_len);
	let mut validity: Option<BooleanBufferBuilder> = None;
	for i in 0..total_len {
		let in_then = then_mask.value(i);
		let in_else = !in_then && else_mask.value(i);
		let value = if in_then {
			a_data.get(i).cloned().unwrap_or_default()
		} else if in_else {
			b_data.get(i).cloned().unwrap_or_default()
		} else {
			T::default()
		};
		out.push(value);
		if !in_then && !in_else {
			let v = validity.get_or_insert_with(|| {
				let mut bv = BooleanBufferBuilder::new(total_len);
				bv.append_n(i, true);
				bv
			});
			v.append(false);
		} else if let Some(v) = validity.as_mut() {
			v.append(true);
		}
	}
	(out, validity.map(|mut v| v.finish()))
}

fn temporal_scatter<T>(
	a_data: &[T],
	b_data: &[T],
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> (Vec<T>, Option<BooleanBuffer>)
where
	T: IsTemporal + Clone + Default + Debug,
{
	let mut out: Vec<T> = Vec::with_capacity(total_len);
	let mut validity: Option<BooleanBufferBuilder> = None;
	for i in 0..total_len {
		let in_then = then_mask.value(i);
		let in_else = !in_then && else_mask.value(i);
		let value = if in_then {
			a_data.get(i).cloned().unwrap_or_default()
		} else if in_else {
			b_data.get(i).cloned().unwrap_or_default()
		} else {
			T::default()
		};
		out.push(value);
		if !in_then && !in_else {
			let v = validity.get_or_insert_with(|| {
				let mut bv = BooleanBufferBuilder::new(total_len);
				bv.append_n(i, true);
				bv
			});
			v.append(false);
		} else if let Some(v) = validity.as_mut() {
			v.append(true);
		}
	}
	(out, validity.map(|mut v| v.finish()))
}

fn uuid_scatter<T>(
	a_data: &[T],
	b_data: &[T],
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> (Vec<T>, Option<BooleanBuffer>)
where
	T: IsUuid + Clone + Default + Debug,
{
	let mut out: Vec<T> = Vec::with_capacity(total_len);
	let mut validity: Option<BooleanBufferBuilder> = None;
	for i in 0..total_len {
		let in_then = then_mask.value(i);
		let in_else = !in_then && else_mask.value(i);
		let value = if in_then {
			a_data.get(i).cloned().unwrap_or_default()
		} else if in_else {
			b_data.get(i).cloned().unwrap_or_default()
		} else {
			T::default()
		};
		out.push(value);
		if !in_then && !in_else {
			let v = validity.get_or_insert_with(|| {
				let mut bv = BooleanBufferBuilder::new(total_len);
				bv.append_n(i, true);
				bv
			});
			v.append(false);
		} else if let Some(v) = validity.as_mut() {
			v.append(true);
		}
	}
	(out, validity.map(|mut v| v.finish()))
}

#[cfg(test)]
mod tests {
	use arrow_buffer::{BooleanBuffer, NullBuffer};
	use reifydb_value::value::{Value, value_type::ValueType};

	use crate::value::column::ColumnBuffer;

	#[test]
	fn scatter_merge_all_mapped_int4() {
		let a = ColumnBuffer::int4([10, 20, 30, 40]);
		let b = ColumnBuffer::int4([90, 80, 70, 60]);
		let then_mask = BooleanBuffer::from(vec![true, false, true, false]);
		let else_mask = BooleanBuffer::from(vec![false, true, false, true]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 4);
		assert!(matches!(merged, ColumnBuffer::Int4(_)));
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(80));
		assert_eq!(merged.get_value(2), Value::Int4(30));
		assert_eq!(merged.get_value(3), Value::Int4(60));
	}

	#[test]
	fn scatter_merge_unmapped_promotes_to_option() {
		let a = ColumnBuffer::int4([10, 20, 30]);
		let b = ColumnBuffer::int4([90, 80, 70]);
		// Row 1 is in neither mask, so it must come out as none.
		let then_mask = BooleanBuffer::from(vec![true, false, true]);
		let else_mask = BooleanBuffer::from(vec![false, false, false]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 3);
		assert!(merged.nulls().is_some());
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::none_of(ValueType::Int4));
		assert_eq!(merged.get_value(2), Value::Int4(30));
	}

	#[test]
	fn scatter_merge_bool_all_mapped() {
		let a = ColumnBuffer::bool([true, true, false, false]);
		let b = ColumnBuffer::bool([false, false, true, true]);
		let then_mask = BooleanBuffer::from(vec![true, false, true, false]);
		let else_mask = BooleanBuffer::from(vec![false, true, false, true]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 4);
		assert!(matches!(merged, ColumnBuffer::Bool(_)));
		assert_eq!(merged.get_value(0), Value::Boolean(true));
		assert_eq!(merged.get_value(1), Value::Boolean(false));
		assert_eq!(merged.get_value(2), Value::Boolean(false));
		assert_eq!(merged.get_value(3), Value::Boolean(true));
	}

	#[test]
	fn scatter_merge_utf8_uses_generic_fallback() {
		let a = ColumnBuffer::utf8(["a", "b", "c"]);
		let b = ColumnBuffer::utf8(["x", "y", "z"]);
		let then_mask = BooleanBuffer::from(vec![true, false, true]);
		let else_mask = BooleanBuffer::from(vec![false, true, false]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 3);
		assert_eq!(merged.get_value(0), Value::Utf8("a".to_string()));
		assert_eq!(merged.get_value(1), Value::Utf8("y".to_string()));
		assert_eq!(merged.get_value(2), Value::Utf8("c".to_string()));
	}
	#[test]
	fn merge_rows_keeps_the_type_default_under_an_unselected_row() {
		// A row taken from the other side must not drag this side's bytes along under its none bit.
		let old = ColumnBuffer::utf8_with_bitvec(["hidden", "kept"], BooleanBuffer::from(vec![false, true]));
		let new = ColumnBuffer::utf8(["fresh", "other"]);
		let mask = BooleanBuffer::from(vec![false, false]);

		let merged = old.merge_rows(&new, &mask, 2);

		let ColumnBuffer::Utf8 {
			container,
			..
		} = &merged
		else {
			panic!("expected a utf8 column");
		};
		assert_eq!(container.value(0), "");
		assert_eq!(merged.get_value(0), Value::none_of(ValueType::Utf8));
		assert_eq!(merged.get_value(1), Value::Utf8("kept".to_string()));
	}

	#[test]
	fn merge_rows_keeps_an_all_valid_column_nullable() {
		// Arrow drops an all-valid null buffer, which would turn a declared Option column into a bare one.
		let old = ColumnBuffer::int4([1, 2]).replace_nulls(Some(NullBuffer::new_valid(2)));
		let new = ColumnBuffer::int4([8, 9]);
		let mask = BooleanBuffer::from(vec![true, false]);

		let merged = old.merge_rows(&new, &mask, 2);

		assert!(merged.nulls().is_some());
		assert_eq!(merged.get_value(0), Value::Int4(8));
		assert_eq!(merged.get_value(1), Value::Int4(2));
	}

	#[test]
	fn scatter_merge_clamps_inputs_shorter_than_the_total_length() {
		// The masks are sized for the whole batch while a side can be shorter, and the rows past its end must
		// read as the type default.
		let a = ColumnBuffer::int4([10, 20]);
		let b = ColumnBuffer::int4([90, 80]);
		let then_mask = BooleanBuffer::from(vec![true, false, true]);
		let else_mask = BooleanBuffer::from(vec![false, true, false]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 3);

		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(80));
		assert_eq!(merged.get_value(2), Value::Int4(0));
	}
}
