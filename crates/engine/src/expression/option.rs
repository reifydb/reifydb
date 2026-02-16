// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{fragment::Fragment, util::bitvec::BitVec, value::r#type::Type};

/// Returns true if the bitvec is Some and has zero ones (all false = all None).
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

pub(crate) fn apply_option_bitvec(result: ColumnData, bitvec: BitVec) -> ColumnData {
	match result {
		ColumnData::Option {
			inner,
			bitvec: existing,
		} => {
			let combined = existing.and(&bitvec);
			ColumnData::Option {
				inner,
				bitvec: combined,
			}
		}
		other => ColumnData::Option {
			inner: Box::new(other),
			bitvec,
		},
	}
}

pub(crate) fn binary_op_unwrap_option(
	left: &Column,
	right: &Column,
	fragment: Fragment,
	inner: impl FnOnce(&Column, &Column) -> crate::Result<Column>,
) -> crate::Result<Column> {
	let (left_data, left_bv) = left.data().unwrap_option();
	let (right_data, right_bv) = right.data().unwrap_option();

	// Short-circuit: if either operand is all-None, return an all-None result
	if is_all_none(left_bv) || is_all_none(right_bv) {
		let len = left_data.len();
		return Ok(Column::new(fragment, ColumnData::none_typed(Type::Boolean, len)));
	}

	let combined_bv = combine_option_bitvecs(left_bv, right_bv);

	let l = Column::new(left.name().clone(), left_data.clone());
	let r = Column::new(right.name().clone(), right_data.clone());

	let result = inner(&l, &r)?;

	Ok(match combined_bv {
		Some(bv) => result.with_new_data(apply_option_bitvec(result.data().clone(), bv)),
		None => result,
	})
}

pub(crate) fn unary_op_unwrap_option(
	col: &Column,
	inner: impl FnOnce(&Column) -> crate::Result<Column>,
) -> crate::Result<Column> {
	let (inner_data, bv) = col.data().unwrap_option();

	// Short-circuit: if all-None, return an all-None result
	if is_all_none(bv) {
		let len = inner_data.len();
		return Ok(Column::new(col.name().clone(), ColumnData::none_typed(Type::Boolean, len)));
	}

	let unwrapped = Column::new(col.name().clone(), inner_data.clone());

	let result = inner(&unwrapped)?;

	Ok(match bv {
		Some(bv) => result.with_new_data(apply_option_bitvec(result.data().clone(), bv.clone())),
		None => result,
	})
}
