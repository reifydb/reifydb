// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_core::value::{
	batch::{batch, filter},
	column::{
		builder::ColumnBuilder,
		factory::{none, none_typed},
		nulls::{split_nulls, with_nulls},
	},
};
use reifydb_value::{
	fragment::Fragment,
	util::bitmap::and_nulls,
	value::{column_view::ColumnView, value_type::ValueType},
};

use crate::Result;

pub(crate) fn is_all_none(nulls: Option<&NullBuffer>) -> bool {
	nulls.is_some_and(|nulls| nulls.null_count() == nulls.len())
}

pub(crate) fn combine_option_bitvecs(a: Option<&NullBuffer>, b: Option<&NullBuffer>) -> Option<NullBuffer> {
	match (a, b) {
		(Some(a), Some(b)) => Some(and_nulls(a, b)),
		(Some(a), None) => Some(a.clone()),
		(None, Some(b)) => Some(b.clone()),
		(None, None) => None,
	}
}

fn filter_column(column: (FieldRef, ArrayRef), mask: &BooleanBuffer) -> Result<(FieldRef, ArrayRef)> {
	let filtered = filter(&batch(vec![column])?, mask)?;
	Ok((filtered.schema_ref().fields()[0].clone(), filtered.column(0).clone()))
}

fn empty_like(column: &(FieldRef, ArrayRef)) -> Result<(FieldRef, ArrayRef)> {
	Ok(ColumnBuilder::like(&ColumnView::try_from(column)?, 0).finish(column.0.name()))
}

pub(crate) fn arith_op_unwrap_option(
	left: &(FieldRef, ArrayRef),
	right: &(FieldRef, ArrayRef),
	fragment: Fragment,
	inner: impl FnOnce(&(FieldRef, ArrayRef), &(FieldRef, ArrayRef)) -> Result<(FieldRef, ArrayRef)>,
) -> Result<(FieldRef, ArrayRef)> {
	let (left_data, left_nulls) = split_nulls(left.clone())?;
	let (right_data, right_nulls) = split_nulls(right.clone())?;
	let typed =
		match (ColumnView::try_from(left)?.is_untyped_none(), ColumnView::try_from(right)?.is_untyped_none()) {
			(true, true) => return Ok(none(fragment.text(), left_data.1.len())),
			(true, false) => Some(&right_data),
			(false, true) => Some(&left_data),
			(false, false) => None,
		};
	if let Some(typed) = typed {
		return Ok(none_typed(fragment.text(), ColumnView::try_from(typed)?.get_type(), left_data.1.len()));
	}

	if is_all_none(left_nulls.as_ref()) || is_all_none(right_nulls.as_ref()) {
		return binary_op_unwrap_option(left, right, fragment, inner);
	}

	let Some(nulls) = combine_option_bitvecs(left_nulls.as_ref(), right_nulls.as_ref()) else {
		return binary_op_unwrap_option(left, right, fragment, inner);
	};

	let defined_left = filter_column(left_data, nulls.inner())?;
	let defined_right = filter_column(right_data, nulls.inner())?;

	let computed = inner(&defined_left, &defined_right)?;
	let result = ColumnView::try_from(&computed)?;

	if result.is_empty() {
		return Ok(none_typed(fragment.text(), result.get_type(), nulls.len()));
	}

	let placeholder = result.get_value(0);
	let mut builder = ColumnBuilder::with_capacity(result.get_type(), nulls.len());
	let mut defined = 0;
	for row in 0..nulls.len() {
		if nulls.is_null(row) {
			builder.push_value(placeholder.clone());
		} else {
			builder.push_value(result.get_value(defined));
			defined += 1;
		}
	}

	with_nulls(builder.finish(fragment.text()), nulls)
}

pub(crate) fn binary_op_unwrap_option(
	left: &(FieldRef, ArrayRef),
	right: &(FieldRef, ArrayRef),
	fragment: Fragment,
	inner: impl FnOnce(&(FieldRef, ArrayRef), &(FieldRef, ArrayRef)) -> Result<(FieldRef, ArrayRef)>,
) -> Result<(FieldRef, ArrayRef)> {
	let (left_data, left_nulls) = split_nulls(left.clone())?;
	let (right_data, right_nulls) = split_nulls(right.clone())?;

	if is_all_none(left_nulls.as_ref()) || is_all_none(right_nulls.as_ref()) {
		let ty = if ColumnView::try_from(left)?.is_untyped_none()
			|| ColumnView::try_from(right)?.is_untyped_none()
		{
			ValueType::Boolean
		} else {
			ColumnView::try_from(&inner(&empty_like(&left_data)?, &empty_like(&right_data)?)?)?.get_type()
		};
		return Ok(none_typed(fragment.text(), ty, left_data.1.len()));
	}

	let combined_nulls = combine_option_bitvecs(left_nulls.as_ref(), right_nulls.as_ref());

	let result = inner(&left_data, &right_data)?;

	match combined_nulls {
		Some(nulls) => with_nulls(result, nulls),
		None => Ok(result),
	}
}

pub(crate) fn unary_op_unwrap_option(
	col: &(FieldRef, ArrayRef),
	inner: impl FnOnce(&(FieldRef, ArrayRef)) -> Result<(FieldRef, ArrayRef)>,
) -> Result<(FieldRef, ArrayRef)> {
	let (inner_data, nulls) = split_nulls(col.clone())?;

	if is_all_none(nulls.as_ref()) {
		let ty = if ColumnView::try_from(col)?.is_untyped_none() {
			ValueType::Boolean
		} else {
			ColumnView::try_from(&inner(&empty_like(&inner_data)?)?)?.get_type()
		};
		return Ok(none_typed(col.0.name(), ty, inner_data.1.len()));
	}

	let result = inner(&inner_data)?;

	match nulls {
		Some(nulls) => with_nulls(result, nulls),
		None => Ok(result),
	}
}
