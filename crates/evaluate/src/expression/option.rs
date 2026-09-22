// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder};
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::Result;

fn is_all_none(bv: Option<&BooleanBuffer>) -> bool {
	match bv {
		Some(bv) => !bv.has_true(),
		None => false,
	}
}

fn is_untyped_none(data: &ColumnBuffer, bv: Option<&BooleanBuffer>) -> bool {
	is_all_none(bv) && matches!(data.get_type(), ValueType::Any | ValueType::Boolean)
}

pub(crate) fn combine_option_bitvecs(a: Option<&BooleanBuffer>, b: Option<&BooleanBuffer>) -> Option<BooleanBuffer> {
	match (a, b) {
		(Some(a), Some(b)) => Some(a & b),
		(Some(a), None) => Some(a.clone()),
		(None, Some(b)) => Some(b.clone()),
		(None, None) => None,
	}
}

pub(crate) fn apply_option_bitvec(result: ColumnBuffer, bitvec: BooleanBuffer) -> ColumnBuffer {
	match result {
		ColumnBuffer::Option {
			inner,
			bitvec: existing,
		} => {
			let combined = &existing & &bitvec;
			ColumnBuffer::Option {
				inner,
				bitvec: combined,
			}
		}
		other => ColumnBuffer::Option {
			inner: Box::new(other),
			bitvec,
		},
	}
}

pub(crate) fn arith_op_unwrap_option(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: Fragment,
	inner: impl FnOnce(&ColumnWithName, &ColumnWithName) -> Result<ColumnWithName>,
) -> Result<ColumnWithName> {
	let (left_data, left_bv) = left.data().unwrap_option();
	let (right_data, right_bv) = right.data().unwrap_option();
	let typed = match (is_untyped_none(left_data, left_bv), is_untyped_none(right_data, right_bv)) {
		(true, false) => Some(right_data),
		(false, true) => Some(left_data),
		_ => None,
	};
	match typed {
		Some(typed) => {
			Ok(ColumnWithName::new(fragment, ColumnBuffer::none_typed(typed.get_type(), left_data.len())))
		}
		None => binary_op_unwrap_option(left, right, fragment, inner),
	}
}

pub(crate) fn binary_op_unwrap_option(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: Fragment,
	inner: impl FnOnce(&ColumnWithName, &ColumnWithName) -> Result<ColumnWithName>,
) -> Result<ColumnWithName> {
	let (left_data, left_bv) = left.data().unwrap_option();
	let (right_data, right_bv) = right.data().unwrap_option();

	if is_all_none(left_bv) || is_all_none(right_bv) {
		let ty = if is_untyped_none(left_data, left_bv) || is_untyped_none(right_data, right_bv) {
			ValueType::Boolean
		} else {
			let l = ColumnWithName::new(left.name().clone(), ColumnBuilder::like(left_data, 0).finish());
			let r = ColumnWithName::new(right.name().clone(), ColumnBuilder::like(right_data, 0).finish());
			inner(&l, &r)?.data().get_type()
		};
		return Ok(ColumnWithName::new(fragment, ColumnBuffer::none_typed(ty, left_data.len())));
	}

	let combined_bv = combine_option_bitvecs(left_bv, right_bv);

	let l = ColumnWithName::new(left.name().clone(), left_data.clone());
	let r = ColumnWithName::new(right.name().clone(), right_data.clone());

	let result = inner(&l, &r)?;

	Ok(match combined_bv {
		Some(bv) => result.with_new_data(apply_option_bitvec(result.data().clone(), bv)),
		None => result,
	})
}

pub(crate) fn unary_op_unwrap_option(
	col: &ColumnWithName,
	inner: impl FnOnce(&ColumnWithName) -> Result<ColumnWithName>,
) -> Result<ColumnWithName> {
	let (inner_data, bv) = col.data().unwrap_option();

	if is_all_none(bv) {
		let ty = if is_untyped_none(inner_data, bv) {
			ValueType::Boolean
		} else {
			inner(&ColumnWithName::new(col.name().clone(), ColumnBuilder::like(inner_data, 0).finish()))?
				.data()
				.get_type()
		};
		return Ok(ColumnWithName::new(col.name().clone(), ColumnBuffer::none_typed(ty, inner_data.len())));
	}

	let unwrapped = ColumnWithName::new(col.name().clone(), inner_data.clone());

	let result = inner(&unwrapped)?;

	Ok(match bv {
		Some(bv) => result.with_new_data(apply_option_bitvec(result.data().clone(), bv.clone())),
		None => result,
	})
}
