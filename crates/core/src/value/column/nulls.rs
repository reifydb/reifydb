// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, make_array, new_null_array};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, FieldRef};
use reifydb_value::{
	Result,
	util::bitmap,
	value::{
		column_view::{ColumnView, ViewData},
		container::{fixed_array, wide_int_array::wide_array},
		value_type::ValueType,
	},
};

use crate::value::batch::frame_error;

pub fn split_nulls(column: (FieldRef, ArrayRef)) -> Result<((FieldRef, ArrayRef), Option<NullBuffer>)> {
	let (field, array) = column;
	let keeps_own = {
		let view = ColumnView::try_from((&array, field.as_ref()))?;
		matches!(view.data, ViewData::Any { .. } | ViewData::Digest { .. } | ViewData::None { .. })
	};
	let nulls = match array.logical_nulls() {
		Some(nulls) => Some(nulls),
		None if field.is_nullable() => Some(NullBuffer::new_valid(array.len())),
		None => None,
	};
	if keeps_own || nulls.is_none() {
		return Ok(((field, array), nulls));
	}
	let data = array.to_data().into_builder().nulls(None).build().map_err(frame_error)?;
	let field = Arc::new(field.as_ref().clone().with_nullable(false));
	Ok(((field, make_array(data)), nulls))
}

pub fn with_nulls(column: (FieldRef, ArrayRef), nulls: NullBuffer) -> Result<(FieldRef, ArrayRef)> {
	let (field, array) = column;
	let len = array.len();
	assert_eq!(nulls.len(), len, "validity of {} rows does not match a column of {len} rows", nulls.len());
	let field = Arc::new(field.as_ref().clone().with_nullable(true));
	if array.data_type().is_null() {
		return Ok((field, array));
	}
	let nulls = match array.logical_nulls() {
		Some(existing) => bitmap::and_nulls(&existing, &nulls),
		None => nulls,
	};
	let data = array.to_data().into_builder().nulls(Some(nulls)).build().map_err(frame_error)?;
	Ok((field, make_array(data)))
}

pub fn none_filler(ty: &ValueType, data_type: &DataType) -> ArrayRef {
	match ty.inner_type() {
		ValueType::Int16 => {
			Arc::new(fixed_array::attach_nulls(wide_array([0i128]), Some(NullBuffer::new_null(1))))
		}
		_ => new_null_array(data_type, 1),
	}
}

#[cfg(test)]
mod tests {
	use arrow_buffer::{BooleanBuffer, NullBuffer};
	use reifydb_value::value::{Value, column_view::ColumnView, value_type::ValueType};

	use super::{split_nulls, with_nulls};
	use crate::value::column::factory::{any_optional, int4, int4_with_bitvec, none};

	#[test]
	fn split_then_with_nulls_round_trips_an_optional_column() {
		// Losing the stripped validity would turn the none row into its hidden default value.
		let column = int4_with_bitvec("a", [1, 2], BooleanBuffer::from(vec![true, false]));

		let (bare, nulls) = split_nulls(column).unwrap();
		assert!(!bare.0.is_nullable());
		assert_eq!(ColumnView::try_from(&bare).unwrap().get_type(), ValueType::Int4);

		let restored = with_nulls(bare, nulls.unwrap()).unwrap();
		let view = ColumnView::try_from(&restored).unwrap();
		assert_eq!(view.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
		assert_eq!(view.get_value(0), Value::Int4(1));
		assert_eq!(view.get_value(1), Value::none_of(ValueType::Int4));
	}

	#[test]
	fn split_keeps_the_nulls_inside_an_any_column() {
		// An any column stores none rows inside its own encoding, so stripping them would corrupt the rows.
		let column = any_optional("a", [Some(Value::Int4(1)), None]);

		let (kept, nulls) = split_nulls(column).unwrap();

		assert!(nulls.is_some());
		assert_eq!(ColumnView::try_from(&kept).unwrap().get_value(1), Value::none_of(ValueType::Any));
	}

	#[test]
	fn split_reports_all_valid_nulls_for_a_nullable_column_without_a_buffer() {
		// A nullable field with no none row must stay optional when the nulls are put back.
		let column = with_nulls(int4("a", [1, 2]), NullBuffer::new_valid(2)).unwrap();

		let (bare, nulls) = split_nulls(column).unwrap();

		assert!(!bare.0.is_nullable());
		assert_eq!(nulls.map(|nulls| nulls.null_count()), Some(0));
	}

	#[test]
	fn with_nulls_keeps_existing_none_rows() {
		// Replacing instead of combining would resurrect a row that was already none.
		let column = int4_with_bitvec("a", [1, 2, 3], BooleanBuffer::from(vec![false, true, true]));

		let merged = with_nulls(column, NullBuffer::new(BooleanBuffer::from(vec![true, true, false]))).unwrap();

		let view = ColumnView::try_from(&merged).unwrap();
		assert_eq!(view.get_value(0), Value::none_of(ValueType::Int4));
		assert_eq!(view.get_value(1), Value::Int4(2));
		assert_eq!(view.get_value(2), Value::none_of(ValueType::Int4));
	}

	#[test]
	fn split_keeps_a_none_column_whole() {
		// An untyped none column has no values to strip, so it must come back unchanged with all rows none.
		let (kept, nulls) = split_nulls(none("a", 2)).unwrap();

		assert!(ColumnView::try_from(&kept).unwrap().is_none());
		assert_eq!(nulls.map(|nulls| nulls.null_count()), Some(2));
	}
}
