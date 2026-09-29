// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, sync::Arc};

use arrow_array::{Array, ArrayRef, BooleanArray, make_array, new_null_array};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	util::{bitmap, kernel},
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		container::{
			decimal_array::decimals,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{uuid4s, uuid7s},
			wide_int_array::wides,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		is::{IsNumber, IsTemporal, IsUuid},
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::field::{from_field, to_field},
	},
};

use crate::value::{
	batch::frame_error,
	column::{
		builder::ColumnBuilder,
		factory,
		nulls::{split_nulls, with_nulls},
	},
};

pub fn merge_rows(
	old: &ColumnView,
	new: &ColumnView,
	mask: &BooleanBuffer,
	len: usize,
	name: &str,
) -> Result<(FieldRef, ArrayRef)> {
	match (old.is_none(), new.is_none()) {
		(true, true) => return Ok(factory::none(name, len)),
		(true, false) => {
			let typed = factory::none_typed(name, new.get_type(), old.len());
			return merge_rows(&ColumnView::try_from(&typed)?, new, mask, len, name);
		}
		(false, true) => {
			let typed = factory::none_typed(name, old.get_type(), new.len());
			return merge_rows(old, &ColumnView::try_from(&typed)?, mask, len, name);
		}
		(false, false) => {}
	}
	if !alignable(old, new, len) || mask.len() < len {
		return Ok(merge_rows_by_value(old, new, mask, len, name));
	}

	let old_side = normalized(old, len);
	let new_side = normalized(new, len);
	let picker = BooleanArray::new(bitmap::resize(mask, len), None);
	let merged = kernel::merged(&picker, new_side.as_ref(), old_side.as_ref());
	let valid = BooleanBuffer::collect_bool(len, |row| match picker.value(row) {
		true => !new.none_at(row),
		false => !old.none_at(row),
	});
	finish_merge(old, merged, valid, name)
}

pub fn scatter_merge(
	then: &ColumnView,
	other: &ColumnView,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
	name: &str,
) -> Result<(FieldRef, ArrayRef)> {
	match (then.is_none(), other.is_none()) {
		(true, true) => return Ok(factory::none(name, total_len)),
		(true, false) => {
			let typed = factory::none_typed(name, other.get_type(), then.len());
			return scatter_merge(
				&ColumnView::try_from(&typed)?,
				other,
				then_mask,
				else_mask,
				total_len,
				name,
			);
		}
		(false, true) => {
			let typed = factory::none_typed(name, then.get_type(), other.len());
			return scatter_merge(
				then,
				&ColumnView::try_from(&typed)?,
				then_mask,
				else_mask,
				total_len,
				name,
			);
		}
		(false, false) => {}
	}

	match (validity(then), validity(other)) {
		(Some(a_nulls), Some(b_nulls)) if !keeps_own_nulls(then) && !keeps_own_nulls(other) => {
			let (a_inner, _) = split_nulls(owned(then))?;
			let (b_inner, _) = split_nulls(owned(other))?;
			let merged_inner = scatter_merge(
				&ColumnView::try_from(&a_inner)?,
				&ColumnView::try_from(&b_inner)?,
				then_mask,
				else_mask,
				total_len,
				name,
			)?;
			let merged = merge_validity_bitvecs(
				a_nulls.inner(),
				b_nulls.inner(),
				then_mask,
				else_mask,
				total_len,
			);
			return with_nulls(merged_inner, NullBuffer::new(merged));
		}
		(None, None) => {
			if let Some(result) = scatter_merge_typed(then, other, then_mask, else_mask, total_len, name)? {
				return Ok(result);
			}
		}
		_ => {}
	}

	let merged = scatter_merge_generic(then, other, then_mask, else_mask, total_len, name)?;
	carry_dictionary_id(merged, then, other)
}

fn validity(view: &ColumnView) -> Option<NullBuffer> {
	match view.logical_nulls() {
		Some(nulls) => Some(nulls),
		None if view.is_nullable() => Some(NullBuffer::new_valid(view.len())),
		None => None,
	}
}

fn keeps_own_nulls(view: &ColumnView) -> bool {
	matches!(view.data, ViewData::Any { .. } | ViewData::Digest { .. })
}

fn owned(view: &ColumnView) -> (FieldRef, ArrayRef) {
	(Arc::new(view.field.clone()), make_array(view.array().to_data()))
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
	then: &ColumnView,
	other: &ColumnView,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
	name: &str,
) -> Result<(FieldRef, ArrayRef)> {
	if !alignable(then, other, total_len) || then_mask.len() < total_len || else_mask.len() < total_len {
		return Ok(scatter_merge_by_value(then, other, then_mask, else_mask, total_len, name));
	}

	let then_side = normalized(then, total_len);
	let else_side = normalized(other, total_len);
	let filler = new_null_array(then_side.data_type(), 1);
	let pairs: Vec<(usize, usize)> = (0..total_len)
		.map(|row| match (then_mask.value(row), else_mask.value(row)) {
			(true, _) => (0, row),
			(false, true) => (1, row),
			(false, false) => (2, 0),
		})
		.collect();
	let merged = kernel::picked(&[then_side.as_ref(), else_side.as_ref(), filler.as_ref()], &pairs);
	let valid = BooleanBuffer::collect_bool(total_len, |row| match (then_mask.value(row), else_mask.value(row)) {
		(true, _) => !then.none_at(row),
		(false, true) => !other.none_at(row),
		(false, false) => false,
	});
	finish_merge(then, merged, valid, name)
}

fn scatter_merge_by_value(
	then: &ColumnView,
	other: &ColumnView,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
	name: &str,
) -> (FieldRef, ArrayRef) {
	let result_type = then.get_type();
	let mut builder = ColumnBuilder::with_capacity(result_type.clone(), total_len);
	for i in 0..total_len {
		if then_mask.value(i) {
			builder.push_value(then.get_value(i));
		} else if else_mask.value(i) {
			builder.push_value(other.get_value(i));
		} else {
			builder.push_value(Value::none_of(result_type.clone()));
		}
	}
	builder.finish(name)
}

fn merge_rows_by_value(
	old: &ColumnView,
	new: &ColumnView,
	mask: &BooleanBuffer,
	len: usize,
	name: &str,
) -> (FieldRef, ArrayRef) {
	let mask = bitmap::resize(mask, len);
	let mut builder = ColumnBuilder::with_capacity(old.get_type(), len);
	for row in 0..len {
		match mask.value(row) {
			true => builder.push_value(new.get_value(row)),
			false => builder.push_value(old.get_value(row)),
		}
	}
	builder.finish(name)
}

fn alignable(old: &ColumnView, new: &ColumnView, len: usize) -> bool {
	old.len() == len && new.len() == len && old.array().data_type() == new.array().data_type()
}

fn normalized(source: &ColumnView, len: usize) -> ArrayRef {
	let array = source.array();
	if source.none_count() == 0 {
		return make_array(array.to_data());
	}
	let filler = new_null_array(array.data_type(), 1);
	let pairs: Vec<(usize, usize)> = (0..len)
		.map(|row| match source.none_at(row) {
			true => (1, 0),
			false => (0, row),
		})
		.collect();
	kernel::picked(&[array, filler.as_ref()], &pairs)
}

fn finish_merge(
	source: &ColumnView,
	merged: ArrayRef,
	valid: BooleanBuffer,
	name: &str,
) -> Result<(FieldRef, ArrayRef)> {
	let optional = validity(source).is_some() || valid.count_set_bits() != valid.len();
	let nulls = optional.then(|| NullBuffer::new(valid));
	let data = merged.to_data().into_builder().nulls(nulls).build().map_err(frame_error)?;
	let (shell, _) = ColumnBuilder::with_capacity(source.base_type(), 0).finish(name);
	Ok((Arc::new(shell.as_ref().clone().with_nullable(optional)), make_array(data)))
}

fn carry_dictionary_id(merged: (FieldRef, ArrayRef), a: &ColumnView, b: &ColumnView) -> Result<(FieldRef, ArrayRef)> {
	let (
		ViewData::DictionaryId {
			dictionary_id: left,
			..
		},
		ViewData::DictionaryId {
			dictionary_id: right,
			..
		},
	) = (&a.data, &b.data)
	else {
		return Ok(merged);
	};
	if left != right || left.is_none() {
		return Ok(merged);
	}
	let (field, array) = merged;
	let mut field_type = from_field(&field)?;
	if field_type.dictionary_id.is_some() {
		return Ok((field, array));
	}
	field_type.dictionary_id = *left;
	Ok((Arc::new(to_field(field.name(), &field_type)), array))
}

fn scatter_merge_typed(
	then: &ColumnView,
	other: &ColumnView,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
	name: &str,
) -> Result<Option<(FieldRef, ArrayRef)>> {
	macro_rules! native_kernel {
		($variant:ident, $t:ty, $build:path) => {
			if let (ViewData::$variant(a), ViewData::$variant(b)) = (&then.data, &other.data) {
				let (data, validity) =
					number_scatter::<$t>(a.values(), b.values(), then_mask, else_mask, total_len);
				return finalize($build(name, data), validity).map(Some);
			}
		};
	}
	macro_rules! number_kernel {
		($variant:ident, $t:ty, $values:path, $build:path) => {
			if let (ViewData::$variant(a), ViewData::$variant(b)) = (&then.data, &other.data) {
				let (data, validity) =
					number_scatter::<$t>(&$values(a), &$values(b), then_mask, else_mask, total_len);
				return finalize($build(name, data), validity).map(Some);
			}
		};
	}
	macro_rules! family_kernel {
		($variant:ident, $t:ty, $values:ident, |$a:ident, $data:ident| $build:expr) => {
			if let (ViewData::$variant($a), ViewData::$variant(b)) = (&then.data, &other.data)
				&& $a.data_type() == b.data_type()
			{
				let ($data, validity) = number_scatter::<$t>(
					&$values(*$a),
					&$values(*b),
					then_mask,
					else_mask,
					total_len,
				);
				return finalize($build, validity).map(Some);
			}
		};
	}
	macro_rules! temporal_kernel {
		($variant:ident, $t:ty, $typed:ident, $build:path) => {
			if let (ViewData::$variant(a), ViewData::$variant(b)) = (&then.data, &other.data) {
				let (data, validity) =
					temporal_scatter::<$t>($typed(a), $typed(b), then_mask, else_mask, total_len);
				return finalize($build(name, data), validity).map(Some);
			}
		};
	}
	macro_rules! uuid_kernel {
		($variant:ident, $t:ty, $typed:ident, $build:path) => {
			if let (ViewData::$variant(a), ViewData::$variant(b)) = (&then.data, &other.data) {
				let (data, validity) =
					uuid_scatter::<$t>($typed(a), $typed(b), then_mask, else_mask, total_len);
				return finalize($build(name, data), validity).map(Some);
			}
		};
	}

	if let (ViewData::Bool(a), ViewData::Bool(b)) = (&then.data, &other.data) {
		let (data, validity) = bool_scatter(a, b, then_mask, else_mask, total_len);
		return finalize(factory::bool(name, data.iter()), validity).map(Some);
	}
	native_kernel!(Float4, f32, factory::float4);
	native_kernel!(Float8, f64, factory::float8);
	native_kernel!(Int1, i8, factory::int1);
	native_kernel!(Int2, i16, factory::int2);
	native_kernel!(Int4, i32, factory::int4);
	native_kernel!(Int8, i64, factory::int8);
	number_kernel!(Int16, i128, wides::<i128>, factory::int16);
	native_kernel!(Uint1, u8, factory::uint1);
	native_kernel!(Uint2, u16, factory::uint2);
	native_kernel!(Uint4, u32, factory::uint4);
	native_kernel!(Uint8, u64, factory::uint8);
	number_kernel!(Uint16, u128, wides::<u128>, factory::uint16);
	family_kernel!(Decimal, Decimal, decimals, |a, data| factory::decimal(name, a.precision(), a.scale(), data));
	temporal_kernel!(Date, Date, dates, factory::date);
	temporal_kernel!(DateTime, DateTime, datetimes, factory::datetime);
	temporal_kernel!(Time, Time, times, factory::time);
	temporal_kernel!(Duration, Duration, durations, factory::duration);
	uuid_kernel!(Uuid4, Uuid4, uuid4s, factory::uuid4);
	uuid_kernel!(Uuid7, Uuid7, uuid7s, factory::uuid7);
	Ok(None)
}

fn finalize(inner: (FieldRef, ArrayRef), validity: Option<BooleanBuffer>) -> Result<(FieldRef, ArrayRef)> {
	match validity {
		Some(bv) => with_nulls(inner, NullBuffer::new(bv)),
		None => Ok(inner),
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
	use reifydb_value::value::{
		Value,
		column_view::{ColumnView, ViewData},
		value_type::ValueType,
	};

	use super::{merge_rows, scatter_merge};
	use crate::value::column::{factory, nulls::with_nulls};

	#[test]
	fn scatter_merge_all_mapped_int4() {
		let a = factory::int4("a", [10, 20, 30, 40]);
		let b = factory::int4("b", [90, 80, 70, 60]);
		let then_mask = BooleanBuffer::from(vec![true, false, true, false]);
		let else_mask = BooleanBuffer::from(vec![false, true, false, true]);

		let merged = scatter_merge(
			&ColumnView::try_from(&a).unwrap(),
			&ColumnView::try_from(&b).unwrap(),
			&then_mask,
			&else_mask,
			4,
			"a",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();
		assert!(matches!(merged.data, ViewData::Int4(_)));
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(80));
		assert_eq!(merged.get_value(2), Value::Int4(30));
		assert_eq!(merged.get_value(3), Value::Int4(60));
	}

	#[test]
	fn scatter_merge_unmapped_promotes_to_option() {
		let a = factory::int4("a", [10, 20, 30]);
		let b = factory::int4("b", [90, 80, 70]);
		// Row 1 is in neither mask, so it must come out as none.
		let then_mask = BooleanBuffer::from(vec![true, false, true]);
		let else_mask = BooleanBuffer::from(vec![false, false, false]);

		let merged = scatter_merge(
			&ColumnView::try_from(&a).unwrap(),
			&ColumnView::try_from(&b).unwrap(),
			&then_mask,
			&else_mask,
			3,
			"a",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();
		assert!(merged.logical_nulls().is_some());
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::none_of(ValueType::Int4));
		assert_eq!(merged.get_value(2), Value::Int4(30));
	}

	#[test]
	fn scatter_merge_bool_all_mapped() {
		let a = factory::bool("a", [true, true, false, false]);
		let b = factory::bool("b", [false, false, true, true]);
		let then_mask = BooleanBuffer::from(vec![true, false, true, false]);
		let else_mask = BooleanBuffer::from(vec![false, true, false, true]);

		let merged = scatter_merge(
			&ColumnView::try_from(&a).unwrap(),
			&ColumnView::try_from(&b).unwrap(),
			&then_mask,
			&else_mask,
			4,
			"a",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();
		assert!(matches!(merged.data, ViewData::Bool(_)));
		assert_eq!(merged.get_value(0), Value::Boolean(true));
		assert_eq!(merged.get_value(1), Value::Boolean(false));
		assert_eq!(merged.get_value(2), Value::Boolean(false));
		assert_eq!(merged.get_value(3), Value::Boolean(true));
	}

	#[test]
	fn scatter_merge_utf8_uses_generic_fallback() {
		let a = factory::utf8("a", ["a", "b", "c"]);
		let b = factory::utf8("b", ["x", "y", "z"]);
		let then_mask = BooleanBuffer::from(vec![true, false, true]);
		let else_mask = BooleanBuffer::from(vec![false, true, false]);

		let merged = scatter_merge(
			&ColumnView::try_from(&a).unwrap(),
			&ColumnView::try_from(&b).unwrap(),
			&then_mask,
			&else_mask,
			3,
			"a",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();
		assert_eq!(merged.get_value(0), Value::Utf8("a".to_string()));
		assert_eq!(merged.get_value(1), Value::Utf8("y".to_string()));
		assert_eq!(merged.get_value(2), Value::Utf8("c".to_string()));
	}
	#[test]
	fn merge_rows_keeps_the_type_default_under_an_unselected_row() {
		// A row taken from the other side must not drag this side's bytes along under its none bit.
		let old = factory::utf8_with_bitvec("a", ["hidden", "kept"], BooleanBuffer::from(vec![false, true]));
		let new = factory::utf8("a", ["fresh", "other"]);
		let mask = BooleanBuffer::from(vec![false, false]);

		let merged = merge_rows(
			&ColumnView::try_from(&old).unwrap(),
			&ColumnView::try_from(&new).unwrap(),
			&mask,
			2,
			"a",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();

		let ViewData::Utf8 {
			container,
			..
		} = &merged.data
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
		let old = with_nulls(factory::int4("a", [1, 2]), NullBuffer::new_valid(2)).unwrap();
		let new = factory::int4("a", [8, 9]);
		let mask = BooleanBuffer::from(vec![true, false]);

		let merged = merge_rows(
			&ColumnView::try_from(&old).unwrap(),
			&ColumnView::try_from(&new).unwrap(),
			&mask,
			2,
			"a",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();

		assert!(merged.is_nullable());
		assert_eq!(merged.get_value(0), Value::Int4(8));
		assert_eq!(merged.get_value(1), Value::Int4(2));
	}

	#[test]
	fn scatter_merge_clamps_inputs_shorter_than_the_total_length() {
		// A side can be shorter than the masks, and its rows past the end must read as the type default.
		let a = factory::int4("a", [10, 20]);
		let b = factory::int4("b", [90, 80]);
		let then_mask = BooleanBuffer::from(vec![true, false, true]);
		let else_mask = BooleanBuffer::from(vec![false, true, false]);

		let merged = scatter_merge(
			&ColumnView::try_from(&a).unwrap(),
			&ColumnView::try_from(&b).unwrap(),
			&then_mask,
			&else_mask,
			3,
			"a",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();

		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(80));
		assert_eq!(merged.get_value(2), Value::Int4(0));
	}
}
