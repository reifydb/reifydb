// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod any;
pub mod blob;
pub mod boolean;
pub mod convert;
pub mod error;
pub mod number;
pub mod temporal;
pub mod text;
pub mod uuid;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow_buffer::NullBuffer;
use arrow_schema::FieldRef;
use arrow_select::{filter::filter, take::take};
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::{Fragment, LazyFragment},
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		value_type::ValueType,
	},
};

use self::{
	convert::{Convert, TargetConvert},
	uuid::to_uuid,
};
use crate::value::{
	batch::frame_error,
	column::{
		factory::{from_one, none_typed},
		nulls::{split_nulls, with_nulls},
	},
};

pub fn cast_value(value: Value, target: &ValueType) -> Result<Value> {
	if value.get_type() == *target {
		return Ok(value);
	}
	let display = value.to_string();
	let data = from_one("", value);
	let cast = cast_column_data(
		TargetConvert {
			target: None,
		},
		&ColumnView::try_from(&data)?,
		target.clone(),
		|| Fragment::internal(display.clone()),
	)?;
	Ok(ColumnView::try_from(&cast)?.get_value(0))
}

pub fn cast_column_data(
	ctx: impl Convert + Copy,
	data: &ColumnView,
	target: ValueType,
	lazy_fragment: impl LazyFragment + Clone,
) -> Result<(FieldRef, ArrayRef)> {
	cast_column_data_inner(ctx, data, target, &|| lazy_fragment.fragment())
}

fn cast_column_data_inner(
	ctx: impl Convert + Copy,
	data: &ColumnView,
	target: ValueType,
	lazy_fragment: &dyn Fn() -> Fragment,
) -> Result<(FieldRef, ArrayRef)> {
	if data.get_type() == target {
		return Ok(owned_column(data));
	}
	if data.is_nullable() && !keeps_own_nulls(data) {
		let total_len = data.len();
		let nulls = data.logical_nulls().unwrap_or_else(|| NullBuffer::new_valid(total_len));
		let inner_target = match &target {
			ValueType::Option(t) => t.as_ref().clone(),
			other => other.clone(),
		};
		let defined_count = total_len - nulls.null_count();

		if defined_count == 0 {
			return Ok(none_typed(data.field.name(), inner_target, total_len));
		}

		let (inner, _) = split_nulls(owned_column(data))?;

		if defined_count < total_len {
			let compacted = compact(&inner, &nulls)?;
			let cast_compacted = cast_column_data_inner(
				ctx,
				&ColumnView::try_from(&compacted)?,
				inner_target,
				lazy_fragment,
			)?;
			return expand(cast_compacted, &nulls);
		}

		let cast_inner =
			cast_column_data_inner(ctx, &ColumnView::try_from(&inner)?, inner_target, lazy_fragment)?;
		return match cast_inner.0.is_nullable() {
			true => Ok(cast_inner),
			false => with_nulls(cast_inner, nulls),
		};
	}

	if let ValueType::Option(inner_target) = &target {
		let cast_inner = cast_column_data_inner(ctx, data, *inner_target.clone(), lazy_fragment)?;
		return match cast_inner.0.is_nullable() {
			true => Ok(cast_inner),
			false => {
				let len = cast_inner.1.len();
				with_nulls(cast_inner, NullBuffer::new_valid(len))
			}
		};
	}

	let object_type = match data.get_type() {
		ValueType::Option(inner) if keeps_own_nulls(data) => *inner,
		other => other,
	};
	if target == object_type {
		return Ok(owned_column(data));
	}
	match (&object_type, &target) {
		(ValueType::Any, _) => any::from_any(ctx, data, target, lazy_fragment),
		(_, t) if t.is_number() => number::to_number(ctx, data, target, lazy_fragment),
		(_, t) if t.is_blob() => blob::to_blob(data, lazy_fragment),
		(_, t) if t.is_bool() => boolean::to_boolean(data, lazy_fragment),
		(_, t) if t.is_utf8() => text::to_text(data, lazy_fragment),
		(_, t) if t.is_temporal() => temporal::to_temporal(data, target, lazy_fragment),
		(_, ValueType::IdentityId) => to_uuid(data, target, lazy_fragment),
		(ValueType::IdentityId, _) => to_uuid(data, target, lazy_fragment),
		(_, t) if t.is_uuid() => to_uuid(data, target, lazy_fragment),
		(source, t) if source.is_uuid() || t.is_uuid() => to_uuid(data, target, lazy_fragment),
		_ => Err(TypeError::UnsupportedCast {
			from: object_type,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into()),
	}
}

fn keeps_own_nulls(data: &ColumnView) -> bool {
	matches!(data.data, ViewData::Any { .. } | ViewData::Digest { .. })
}

fn owned_column(data: &ColumnView) -> (FieldRef, ArrayRef) {
	(Arc::new(data.field.clone()), data.array().slice(0, data.len()))
}

fn compact(column: &(FieldRef, ArrayRef), nulls: &NullBuffer) -> Result<(FieldRef, ArrayRef)> {
	let predicate = BooleanArray::new(nulls.inner().clone(), None);
	let array = filter(column.1.as_ref(), &predicate).map_err(frame_error)?;
	Ok((column.0.clone(), array))
}

fn expand(column: (FieldRef, ArrayRef), nulls: &NullBuffer) -> Result<(FieldRef, ArrayRef)> {
	let mut src_idx = 0u32;
	let picks: UInt32Array = (0..nulls.len())
		.map(|i| match nulls.is_valid(i) {
			true => {
				src_idx += 1;
				Some(src_idx - 1)
			}
			false => None,
		})
		.collect();
	let (field, array) = column;
	let array = take(array.as_ref(), &picks, None).map_err(frame_error)?;
	Ok((Arc::new(field.as_ref().clone().with_nullable(true)), array))
}

#[cfg(test)]
mod tests {
	use arrow_array::{Array, ArrayRef};
	use arrow_schema::FieldRef;
	use reifydb_value::{
		fragment::Fragment,
		value::{Value, column_view::ColumnView, value_type::ValueType},
	};

	use super::{cast_column_data, convert::TargetConvert};
	use crate::value::column::builder::ColumnBuilder;

	fn optional_int4_with_a_gap() -> (FieldRef, ArrayRef) {
		let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(ValueType::Int4)), 3);
		builder.push_value(Value::Int4(7));
		builder.push_none();
		builder.push_value(Value::Int4(9));
		builder.finish("n")
	}

	#[test]
	fn a_nullable_column_cast_to_its_own_type_is_not_copied() {
		// Insert, update and the flow sinks cast every column to its target even when it already
		// has that type; for a nullable column the null split used to filter and re-expand it, two
		// full copies of data that never changed. The values buffer must be the input's own.
		let input = optional_int4_with_a_gap();
		let view = ColumnView::try_from(&input).unwrap();
		let target = view.get_type();
		let (field, array) = cast_column_data(
			TargetConvert {
				target: None,
			},
			&view,
			target.clone(),
			|| Fragment::None,
		)
		.unwrap();

		assert_eq!(
			array.to_data().buffers()[0].as_ptr(),
			input.1.to_data().buffers()[0].as_ptr(),
			"an equal-type cast must hand back the same values buffer, not a copy"
		);
		let cast = ColumnView::try_from((&array, field.as_ref())).unwrap();
		assert_eq!(cast.get_type(), target);
		assert_eq!(cast.get_value(0), Value::Int4(7));
		assert!(cast.none_at(1), "the gap must survive as none");
		assert_eq!(cast.get_value(2), Value::Int4(9));
	}

	#[test]
	fn a_nullable_column_cast_to_a_wider_type_is_still_converted() {
		// The shortcut must only fire on an exact type match; a nullable Int4 handed through
		// unchanged as Int8 would be read back as the wrong width.
		let input = optional_int4_with_a_gap();
		let view = ColumnView::try_from(&input).unwrap();
		let target = ValueType::Option(Box::new(ValueType::Int8));
		let (field, array) = cast_column_data(
			TargetConvert {
				target: None,
			},
			&view,
			target,
			|| Fragment::None,
		)
		.unwrap();

		let cast = ColumnView::try_from((&array, field.as_ref())).unwrap();
		assert_eq!(cast.get_value(0), Value::Int8(7));
		assert!(cast.none_at(1), "the gap must survive the cast");
		assert_eq!(cast.get_value(2), Value::Int8(9));
	}
}
