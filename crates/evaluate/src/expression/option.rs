// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::NullBuffer;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder};
use reifydb_value::{fragment::Fragment, util::bitmap::and_nulls, value::value_type::ValueType};

use crate::Result;

pub(crate) fn is_all_none(nulls: Option<&NullBuffer>) -> bool {
	nulls.is_some_and(|nulls| nulls.null_count() == nulls.len())
}

pub(crate) fn is_untyped_none(data: &ColumnBuffer, nulls: Option<&NullBuffer>) -> bool {
	is_all_none(nulls) && matches!(data.get_type(), ValueType::Any | ValueType::Boolean)
}

pub(crate) fn combine_option_bitvecs(a: Option<&NullBuffer>, b: Option<&NullBuffer>) -> Option<NullBuffer> {
	match (a, b) {
		(Some(a), Some(b)) => Some(and_nulls(a, b)),
		(Some(a), None) => Some(a.clone()),
		(None, Some(b)) => Some(b.clone()),
		(None, None) => None,
	}
}

pub(crate) fn arith_op_unwrap_option(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: Fragment,
	inner: impl FnOnce(&ColumnWithName, &ColumnWithName) -> Result<ColumnWithName>,
) -> Result<ColumnWithName> {
	let (left_data, left_nulls) = left.data().clone().split_nulls();
	let (right_data, right_nulls) = right.data().clone().split_nulls();
	let typed = match (
		is_untyped_none(&left_data, left_nulls.as_ref()),
		is_untyped_none(&right_data, right_nulls.as_ref()),
	) {
		(true, false) => Some(&right_data),
		(false, true) => Some(&left_data),
		_ => None,
	};
	if let Some(typed) = typed {
		return Ok(ColumnWithName::new(fragment, ColumnBuffer::none_typed(typed.get_type(), left_data.len())));
	}

	if is_all_none(left_nulls.as_ref()) || is_all_none(right_nulls.as_ref()) {
		return binary_op_unwrap_option(left, right, fragment, inner);
	}

	let Some(nulls) = combine_option_bitvecs(left_nulls.as_ref(), right_nulls.as_ref()) else {
		return binary_op_unwrap_option(left, right, fragment, inner);
	};

	let mut defined_left = left_data;
	let mut defined_right = right_data;
	defined_left.filter(nulls.inner())?;
	defined_right.filter(nulls.inner())?;

	let result = inner(
		&ColumnWithName::new(left.name().clone(), defined_left),
		&ColumnWithName::new(right.name().clone(), defined_right),
	)?;

	if result.data().is_empty() {
		return Ok(ColumnWithName::new(
			fragment,
			ColumnBuffer::none_typed(result.data().get_type(), nulls.len()),
		));
	}

	let placeholder = result.data().get_value(0);
	let mut builder = ColumnBuilder::with_capacity(result.data().get_type(), nulls.len());
	let mut defined = 0;
	for row in 0..nulls.len() {
		if nulls.is_null(row) {
			builder.push_value(placeholder.clone());
		} else {
			builder.push_value(result.data().get_value(defined));
			defined += 1;
		}
	}

	Ok(ColumnWithName::new(fragment, builder.finish().with_nulls(nulls)))
}

pub(crate) fn binary_op_unwrap_option(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: Fragment,
	inner: impl FnOnce(&ColumnWithName, &ColumnWithName) -> Result<ColumnWithName>,
) -> Result<ColumnWithName> {
	let (left_data, left_nulls) = left.data().clone().split_nulls();
	let (right_data, right_nulls) = right.data().clone().split_nulls();

	if is_all_none(left_nulls.as_ref()) || is_all_none(right_nulls.as_ref()) {
		let ty = if is_untyped_none(&left_data, left_nulls.as_ref())
			|| is_untyped_none(&right_data, right_nulls.as_ref())
		{
			ValueType::Boolean
		} else {
			let l = ColumnWithName::new(left.name().clone(), ColumnBuilder::like(&left_data, 0).finish());
			let r = ColumnWithName::new(right.name().clone(), ColumnBuilder::like(&right_data, 0).finish());
			inner(&l, &r)?.data().get_type()
		};
		return Ok(ColumnWithName::new(fragment, ColumnBuffer::none_typed(ty, left_data.len())));
	}

	let combined_nulls = combine_option_bitvecs(left_nulls.as_ref(), right_nulls.as_ref());

	let l = ColumnWithName::new(left.name().clone(), left_data);
	let r = ColumnWithName::new(right.name().clone(), right_data);

	let result = inner(&l, &r)?;

	Ok(match combined_nulls {
		Some(nulls) => result.with_new_data(result.data().clone().with_nulls(nulls)),
		None => result,
	})
}

pub(crate) fn unary_op_unwrap_option(
	col: &ColumnWithName,
	inner: impl FnOnce(&ColumnWithName) -> Result<ColumnWithName>,
) -> Result<ColumnWithName> {
	let (inner_data, nulls) = col.data().clone().split_nulls();

	if is_all_none(nulls.as_ref()) {
		let ty = if is_untyped_none(&inner_data, nulls.as_ref()) {
			ValueType::Boolean
		} else {
			inner(&ColumnWithName::new(col.name().clone(), ColumnBuilder::like(&inner_data, 0).finish()))?
				.data()
				.get_type()
		};
		return Ok(ColumnWithName::new(col.name().clone(), ColumnBuffer::none_typed(ty, inner_data.len())));
	}

	let unwrapped = ColumnWithName::new(col.name().clone(), inner_data);

	let result = inner(&unwrapped)?;

	Ok(match nulls {
		Some(nulls) => result.with_new_data(result.data().clone().with_nulls(nulls)),
		None => result,
	})
}
