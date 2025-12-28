// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::Ordering;

use reifydb_core::{
	return_error,
	value::{
		column::{Column, ColumnData},
		container::{BoolContainer, NumberContainer, TemporalContainer, Utf8Container},
	},
};
use reifydb_type::{
	Decimal, Fragment, Int, IsNumber, IsTemporal, Promote, Type, Type::Boolean, Uint, diagnostic::Diagnostic,
	value::number,
};

use crate::evaluate::column::ColumnEvaluationContext;

mod between;
mod equal;
mod greater_than;
mod greater_than_equal;
mod r#in;
mod less_than;
mod less_than_equal;
mod not_equal;

// Trait for comparison operations - monomorphized for fast execution
pub(crate) trait CompareOp {
	fn compare_ordering(ordering: Option<Ordering>) -> bool;
	fn compare_bool(_l: bool, _r: bool) -> Option<bool> {
		None
	}
	fn undefined_result() -> bool {
		false
	}
}

pub(crate) struct Equal;
pub(crate) struct NotEqual;
pub(crate) struct GreaterThan;
pub(crate) struct GreaterThanEqual;
pub(crate) struct LessThan;
pub(crate) struct LessThanEqual;

impl CompareOp for Equal {
	#[inline]
	fn compare_ordering(o: Option<Ordering>) -> bool {
		o == Some(Ordering::Equal)
	}
	#[inline]
	fn compare_bool(l: bool, r: bool) -> Option<bool> {
		Some(l == r)
	}
}

impl CompareOp for NotEqual {
	#[inline]
	fn compare_ordering(o: Option<Ordering>) -> bool {
		o != Some(Ordering::Equal)
	}
	#[inline]
	fn compare_bool(l: bool, r: bool) -> Option<bool> {
		Some(l != r)
	}
}

impl CompareOp for GreaterThan {
	#[inline]
	fn compare_ordering(o: Option<Ordering>) -> bool {
		o == Some(Ordering::Greater)
	}
}

impl CompareOp for GreaterThanEqual {
	#[inline]
	fn compare_ordering(o: Option<Ordering>) -> bool {
		matches!(o, Some(Ordering::Greater) | Some(Ordering::Equal))
	}
}

impl CompareOp for LessThan {
	#[inline]
	fn compare_ordering(o: Option<Ordering>) -> bool {
		o == Some(Ordering::Less)
	}
}

impl CompareOp for LessThanEqual {
	#[inline]
	fn compare_ordering(o: Option<Ordering>) -> bool {
		matches!(o, Some(Ordering::Less) | Some(Ordering::Equal))
	}
}

#[inline]
fn compare_number<Op: CompareOp, L, R>(
	ctx: &ColumnEvaluationContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	fragment: Fragment,
) -> Column
where
	L: Promote<R> + IsNumber,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		let data: Vec<bool> =
			l.data().iter()
				.zip(r.data().iter())
				.map(|(l_val, r_val)| Op::compare_ordering(number::partial_cmp(l_val, r_val)))
				.collect();

		Column {
			name: Fragment::internal(fragment.text()),
			data: ColumnData::bool(data),
		}
	} else {
		let mut data = ctx.pooled(Boolean, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					data.push(Op::compare_ordering(number::partial_cmp(l, r)));
				}
				_ => data.push_undefined(),
			}
		}

		Column {
			name: Fragment::internal(fragment.text()),
			data,
		}
	}
}

#[inline]
fn compare_temporal<Op: CompareOp, T>(l: &TemporalContainer<T>, r: &TemporalContainer<T>, fragment: Fragment) -> Column
where
	T: IsTemporal + Copy + PartialOrd,
{
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		let data: Vec<bool> =
			l.data().iter()
				.zip(r.data().iter())
				.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
				.collect();

		Column {
			name: Fragment::internal(fragment.text()),
			data: ColumnData::bool(data),
		}
	} else {
		let mut data = Vec::with_capacity(l.len());
		let mut bitvec = Vec::with_capacity(l.len());

		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					data.push(Op::compare_ordering(l.partial_cmp(r)));
					bitvec.push(true);
				}
				_ => {
					data.push(Op::undefined_result());
					bitvec.push(false);
				}
			}
		}

		Column {
			name: Fragment::internal(fragment.text()),
			data: ColumnData::bool_with_bitvec(data, bitvec),
		}
	}
}

#[inline]
fn compare_utf8<Op: CompareOp>(l: &Utf8Container, r: &Utf8Container, fragment: Fragment) -> Column {
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		let data: Vec<bool> =
			l.data().iter()
				.zip(r.data().iter())
				.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
				.collect();

		Column {
			name: Fragment::internal(fragment.text()),
			data: ColumnData::bool(data),
		}
	} else {
		let mut data = Vec::with_capacity(l.len());
		let mut bitvec = Vec::with_capacity(l.len());

		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					data.push(Op::compare_ordering(l.partial_cmp(r)));
					bitvec.push(true);
				}
				_ => {
					data.push(Op::undefined_result());
					bitvec.push(false);
				}
			}
		}

		Column {
			name: Fragment::internal(fragment.text()),
			data: ColumnData::bool_with_bitvec(data, bitvec),
		}
	}
}

#[inline]
fn compare_bool<Op: CompareOp>(
	ctx: &ColumnEvaluationContext,
	l: &BoolContainer,
	r: &BoolContainer,
	fragment: Fragment,
) -> Option<Column> {
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		let data: Vec<bool> =
			l.data().iter()
				.zip(r.data().iter())
				.filter_map(|(l_val, r_val)| Op::compare_bool(l_val, r_val))
				.collect();

		if data.len() == l.len() {
			Some(Column {
				name: Fragment::internal(fragment.text()),
				data: ColumnData::bool(data),
			})
		} else {
			None
		}
	} else {
		let mut data = ctx.pooled(Boolean, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					if let Some(result) = Op::compare_bool(l, r) {
						data.push(result);
					} else {
						return None;
					}
				}
				_ => data.push_undefined(),
			}
		}

		Some(Column {
			name: Fragment::internal(fragment.text()),
			data,
		})
	}
}

pub(crate) fn compare_columns<Op: CompareOp>(
	ctx: &ColumnEvaluationContext,
	left: &Column,
	right: &Column,
	fragment: Fragment,
	error_fn: impl FnOnce(Fragment, Type, Type) -> Diagnostic,
) -> crate::Result<Column> {
	match (&left.data(), &right.data()) {
		(ColumnData::Bool(l), ColumnData::Bool(r)) => {
			if let Some(col) = compare_bool::<Op>(ctx, l, r, fragment.clone()) {
				return Ok(col);
			}
		}
		// Float4 with Int, Uint, Decimal
		(
			ColumnData::Float4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, f32, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Float4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, f32, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Float4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, f32, Decimal>(ctx, l, r, fragment));
		}
		// Float4
		(ColumnData::Float4(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, f32, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, f32, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, f32, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, f32, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, f32, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, f32, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, f32, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, f32, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, f32, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, f32, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, f32, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, f32, u128>(ctx, l, r, fragment));
		}
		// Float8
		(ColumnData::Float8(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, f64, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, f64, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, f64, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, f64, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, f64, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, f64, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, f64, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, f64, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, f64, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, f64, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, f64, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, f64, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Float8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, f64, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Float8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, f64, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Float8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, f64, Decimal>(ctx, l, r, fragment));
		}
		// Int1
		(ColumnData::Int1(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, i8, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, i8, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, i8, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, i8, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, i8, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, i8, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, i8, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, i8, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, i8, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, i8, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, i8, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, i8, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i8, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i8, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i8, Decimal>(ctx, l, r, fragment));
		}
		// Int2
		(ColumnData::Int2(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, i16, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, i16, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, i16, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, i16, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, i16, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, i16, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, i16, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, i16, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, i16, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, i16, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, i16, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, i16, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i16, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i16, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i16, Decimal>(ctx, l, r, fragment));
		}
		// Int4
		(ColumnData::Int4(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, i32, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, i32, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, i32, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, i32, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, i32, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, i32, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, i32, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, i32, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, i32, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, i32, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, i32, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, i32, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i32, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i32, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i32, Decimal>(ctx, l, r, fragment));
		}
		// Int8
		(ColumnData::Int8(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, i64, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, i64, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, i64, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, i64, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, i64, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, i64, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, i64, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, i64, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, i64, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, i64, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, i64, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, i64, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i64, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i64, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i64, Decimal>(ctx, l, r, fragment));
		}
		// Int16
		(ColumnData::Int16(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, i128, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, i128, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, i128, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, i128, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, i128, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, i128, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, i128, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, i128, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, i128, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, i128, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, i128, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, i128, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i128, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i128, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, i128, Decimal>(ctx, l, r, fragment));
		}
		// Uint1
		(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, u8, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, u8, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, u8, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, u8, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, u8, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, u8, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, u8, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, u8, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, u8, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, u8, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, u8, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, u8, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u8, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u8, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u8, Decimal>(ctx, l, r, fragment));
		}
		// Uint2
		(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, u16, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, u16, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, u16, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, u16, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, u16, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, u16, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, u16, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, u16, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, u16, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, u16, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, u16, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, u16, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u16, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u16, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u16, Decimal>(ctx, l, r, fragment));
		}
		// Uint4
		(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, u32, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, u32, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, u32, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, u32, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, u32, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, u32, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, u32, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, u32, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, u32, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, u32, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, u32, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, u32, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u32, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u32, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u32, Decimal>(ctx, l, r, fragment));
		}
		// Uint8
		(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, u64, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, u64, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, u64, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, u64, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, u64, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, u64, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, u64, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, u64, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, u64, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, u64, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, u64, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, u64, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u64, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u64, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u64, Decimal>(ctx, l, r, fragment));
		}
		// Uint16
		(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
			return Ok(compare_number::<Op, u128, f32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
			return Ok(compare_number::<Op, u128, f64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
			return Ok(compare_number::<Op, u128, i8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
			return Ok(compare_number::<Op, u128, i16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
			return Ok(compare_number::<Op, u128, i32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
			return Ok(compare_number::<Op, u128, i64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
			return Ok(compare_number::<Op, u128, i128>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
			return Ok(compare_number::<Op, u128, u8>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
			return Ok(compare_number::<Op, u128, u16>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
			return Ok(compare_number::<Op, u128, u32>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
			return Ok(compare_number::<Op, u128, u64>(ctx, l, r, fragment));
		}
		(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
			return Ok(compare_number::<Op, u128, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u128, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u128, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, u128, Decimal>(ctx, l, r, fragment));
		}
		// Int
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => {
			return Ok(compare_number::<Op, Int, f32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => {
			return Ok(compare_number::<Op, Int, f64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => {
			return Ok(compare_number::<Op, Int, i8>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => {
			return Ok(compare_number::<Op, Int, i16>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => {
			return Ok(compare_number::<Op, Int, i32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => {
			return Ok(compare_number::<Op, Int, i64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => {
			return Ok(compare_number::<Op, Int, i128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => {
			return Ok(compare_number::<Op, Int, u8>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => {
			return Ok(compare_number::<Op, Int, u16>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => {
			return Ok(compare_number::<Op, Int, u32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => {
			return Ok(compare_number::<Op, Int, u64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => {
			return Ok(compare_number::<Op, Int, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Int, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Int, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Int, Decimal>(ctx, l, r, fragment));
		}
		// Uint
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => {
			return Ok(compare_number::<Op, Uint, f32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => {
			return Ok(compare_number::<Op, Uint, f64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => {
			return Ok(compare_number::<Op, Uint, i8>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => {
			return Ok(compare_number::<Op, Uint, i16>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => {
			return Ok(compare_number::<Op, Uint, i32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => {
			return Ok(compare_number::<Op, Uint, i64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => {
			return Ok(compare_number::<Op, Uint, i128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => {
			return Ok(compare_number::<Op, Uint, u8>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => {
			return Ok(compare_number::<Op, Uint, u16>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => {
			return Ok(compare_number::<Op, Uint, u32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => {
			return Ok(compare_number::<Op, Uint, u64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => {
			return Ok(compare_number::<Op, Uint, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Uint, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Uint, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Uint, Decimal>(ctx, l, r, fragment));
		}
		// Decimal
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => {
			return Ok(compare_number::<Op, Decimal, f32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => {
			return Ok(compare_number::<Op, Decimal, f64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => {
			return Ok(compare_number::<Op, Decimal, i8>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => {
			return Ok(compare_number::<Op, Decimal, i16>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => {
			return Ok(compare_number::<Op, Decimal, i32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => {
			return Ok(compare_number::<Op, Decimal, i64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => {
			return Ok(compare_number::<Op, Decimal, i128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => {
			return Ok(compare_number::<Op, Decimal, u8>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => {
			return Ok(compare_number::<Op, Decimal, u16>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => {
			return Ok(compare_number::<Op, Decimal, u32>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => {
			return Ok(compare_number::<Op, Decimal, u64>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => {
			return Ok(compare_number::<Op, Decimal, u128>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Decimal, Int>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Decimal, Uint>(ctx, l, r, fragment));
		}
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => {
			return Ok(compare_number::<Op, Decimal, Decimal>(ctx, l, r, fragment));
		}
		// Temporal types
		(ColumnData::Date(l), ColumnData::Date(r)) => {
			return Ok(compare_temporal::<Op, _>(l, r, fragment));
		}
		(ColumnData::DateTime(l), ColumnData::DateTime(r)) => {
			return Ok(compare_temporal::<Op, _>(l, r, fragment));
		}
		(ColumnData::Time(l), ColumnData::Time(r)) => {
			return Ok(compare_temporal::<Op, _>(l, r, fragment));
		}
		(ColumnData::Duration(l), ColumnData::Duration(r)) => {
			return Ok(compare_temporal::<Op, _>(l, r, fragment));
		}
		// Utf8
		(
			ColumnData::Utf8 {
				container: l,
				..
			},
			ColumnData::Utf8 {
				container: r,
				..
			},
		) => {
			return Ok(compare_utf8::<Op>(l, r, fragment));
		}
		// Undefined
		(ColumnData::Undefined(container), _) | (_, ColumnData::Undefined(container)) => {
			return Ok(Column {
				name: Fragment::internal(fragment.text()),
				data: ColumnData::bool(vec![Op::undefined_result(); container.len()]),
			});
		}
		_ => {}
	}
	return_error!(error_fn(fragment, left.get_type(), right.get_type()))
}
