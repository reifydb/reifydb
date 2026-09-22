// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use arrow_array::{BooleanArray, Decimal128Array, PrimitiveArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer, ScalarBuffer};
use reifydb_value::value::{
	Value,
	container::{
		decimal_array::{int16_array, u128s, uint16_array},
		temporal_array::{
			date_array, dates, datetime_array, datetimes, duration_array, durations, time_array, times,
		},
		uuid_array::{uuid4_array, uuid4s, uuid7_array, uuid7s},
	},
	date::Date,
	datetime::DateTime,
	duration::Duration,
	is::{IsNumber, IsTemporal, IsUuid},
	time::Time,
	uuid::{Uuid4, Uuid7},
};

use crate::value::column::{ColumnBuffer, builder::ColumnBuilder};

impl ColumnBuffer {
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
	use arrow_buffer::BooleanBuffer;
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
}
