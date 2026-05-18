// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_type::{fragment::Fragment, util::bitvec::BitVec, value::r#type::Type};

use crate::Result;

fn is_all_none(bv: Option<&BitVec>) -> bool {
	match bv {
		Some(bv) => bv.count_ones() == 0,
		None => false,
	}
}

pub(crate) fn combine_option_bitvecs(a: Option<&BitVec>, b: Option<&BitVec>) -> Option<BitVec> {
	match (a, b) {
		(Some(a), Some(b)) => Some(a.and(b)),
		(Some(a), None) => Some(a.clone()),
		(None, Some(b)) => Some(b.clone()),
		(None, None) => None,
	}
}

pub(crate) fn apply_option_bitvec(result: ColumnBuffer, bitvec: BitVec) -> ColumnBuffer {
	match result {
		ColumnBuffer::Option {
			inner,
			bitvec: existing,
		} => {
			let combined = existing.and(&bitvec);
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

pub(crate) fn binary_op_unwrap_option(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: Fragment,
	inner: impl FnOnce(&ColumnWithName, &ColumnWithName) -> Result<ColumnWithName>,
) -> Result<ColumnWithName> {
	let (left_data, left_bv) = left.data().unwrap_option();
	let (right_data, right_bv) = right.data().unwrap_option();

	if is_all_none(left_bv) || is_all_none(right_bv) {
		let len = left_data.len();
		return Ok(ColumnWithName::new(fragment, ColumnBuffer::none_typed(Type::Boolean, len)));
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
		let len = inner_data.len();
		return Ok(ColumnWithName::new(col.name().clone(), ColumnBuffer::none_typed(Type::Boolean, len)));
	}

	let unwrapped = ColumnWithName::new(col.name().clone(), inner_data.clone());

	let result = inner(&unwrapped)?;

	Ok(match bv {
		Some(bv) => result.with_new_data(apply_option_bitvec(result.data().clone(), bv.clone())),
		None => result,
	})
}
