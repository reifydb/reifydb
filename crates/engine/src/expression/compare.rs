// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cmp::Ordering;

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{
	error::Diagnostic,
	fragment::Fragment,
	return_error,
	value::{
		container::{
			blob::BlobContainer, bool::BoolContainer, number::NumberContainer, temporal::TemporalContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		decimal::Decimal,
		int::Int,
		is::{IsNumber, IsTemporal, IsUuid},
		number::{compare::partial_cmp, promote::Promote},
		r#type::Type,
		uint::Uint,
	},
};

/// Generates a complete match expression dispatching all numeric type pairs for comparison.
/// Uses push-down accumulation to build the cross-product of type arms.
macro_rules! dispatch_compare {
	// Entry point
	(
		$left:expr, $right:expr;
		$fragment:expr;
		$($extra:tt)*
	) => {
		dispatch_compare!(@rows
			($left, $right) ($fragment)
			[(Float4, f32) (Float8, f64) (Int1, i8) (Int2, i16) (Int4, i32) (Int8, i64) (Int16, i128) (Uint1, u8) (Uint2, u16) (Uint4, u32) (Uint8, u64) (Uint16, u128)]
			{$($extra)*}
			{}
		)
	};

	// Recursive: process one fixed-left type pair, generating all 15 right-side arms
	(@rows
		($left:expr, $right:expr) ($fragment:expr)
		[($L:ident, $Lt:ty) $($rest:tt)*]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		dispatch_compare!(@rows
			($left, $right) ($fragment)
			[$($rest)*]
			{$($extra)*}
			{
				$($acc)*
				(ColumnData::$L(l), ColumnData::Float4(r)) => { return Ok(compare_number::<Op, $Lt, f32>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Float8(r)) => { return Ok(compare_number::<Op, $Lt, f64>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int1(r)) => { return Ok(compare_number::<Op, $Lt, i8>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int2(r)) => { return Ok(compare_number::<Op, $Lt, i16>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int4(r)) => { return Ok(compare_number::<Op, $Lt, i32>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int8(r)) => { return Ok(compare_number::<Op, $Lt, i64>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int16(r)) => { return Ok(compare_number::<Op, $Lt, i128>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint1(r)) => { return Ok(compare_number::<Op, $Lt, u8>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint2(r)) => { return Ok(compare_number::<Op, $Lt, u16>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint4(r)) => { return Ok(compare_number::<Op, $Lt, u32>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint8(r)) => { return Ok(compare_number::<Op, $Lt, u64>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint16(r)) => { return Ok(compare_number::<Op, $Lt, u128>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Int>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Uint>(l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Decimal>(l, r, $fragment)); },
			}
		)
	};

	// Base case: all fixed-left types processed, emit the match with arb-left arms
	(@rows
		($left:expr, $right:expr) ($fragment:expr)
		[]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		match ($left, $right) {
			// Fixed × all (12 × 15 = 180 arms)
			$($acc)*

			// Int × all (15 arms)
			(ColumnData::Int { container: l, .. }, ColumnData::Float4(r)) => { return Ok(compare_number::<Op, Int, f32>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Float8(r)) => { return Ok(compare_number::<Op, Int, f64>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int1(r)) => { return Ok(compare_number::<Op, Int, i8>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int2(r)) => { return Ok(compare_number::<Op, Int, i16>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int4(r)) => { return Ok(compare_number::<Op, Int, i32>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int8(r)) => { return Ok(compare_number::<Op, Int, i64>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int16(r)) => { return Ok(compare_number::<Op, Int, i128>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint1(r)) => { return Ok(compare_number::<Op, Int, u8>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint2(r)) => { return Ok(compare_number::<Op, Int, u16>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint4(r)) => { return Ok(compare_number::<Op, Int, u32>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint8(r)) => { return Ok(compare_number::<Op, Int, u64>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint16(r)) => { return Ok(compare_number::<Op, Int, u128>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int { container: r, .. }) => { return Ok(compare_number::<Op, Int, Int>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Int, Uint>(l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Int, Decimal>(l, r, $fragment)); },

			// Uint × all (15 arms)
			(ColumnData::Uint { container: l, .. }, ColumnData::Float4(r)) => { return Ok(compare_number::<Op, Uint, f32>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Float8(r)) => { return Ok(compare_number::<Op, Uint, f64>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int1(r)) => { return Ok(compare_number::<Op, Uint, i8>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int2(r)) => { return Ok(compare_number::<Op, Uint, i16>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int4(r)) => { return Ok(compare_number::<Op, Uint, i32>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int8(r)) => { return Ok(compare_number::<Op, Uint, i64>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int16(r)) => { return Ok(compare_number::<Op, Uint, i128>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint1(r)) => { return Ok(compare_number::<Op, Uint, u8>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint2(r)) => { return Ok(compare_number::<Op, Uint, u16>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint4(r)) => { return Ok(compare_number::<Op, Uint, u32>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint8(r)) => { return Ok(compare_number::<Op, Uint, u64>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint16(r)) => { return Ok(compare_number::<Op, Uint, u128>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Int>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Uint>(l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Decimal>(l, r, $fragment)); },

			// Decimal × all (15 arms)
			(ColumnData::Decimal { container: l, .. }, ColumnData::Float4(r)) => { return Ok(compare_number::<Op, Decimal, f32>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Float8(r)) => { return Ok(compare_number::<Op, Decimal, f64>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int1(r)) => { return Ok(compare_number::<Op, Decimal, i8>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int2(r)) => { return Ok(compare_number::<Op, Decimal, i16>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int4(r)) => { return Ok(compare_number::<Op, Decimal, i32>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int8(r)) => { return Ok(compare_number::<Op, Decimal, i64>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int16(r)) => { return Ok(compare_number::<Op, Decimal, i128>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint1(r)) => { return Ok(compare_number::<Op, Decimal, u8>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint2(r)) => { return Ok(compare_number::<Op, Decimal, u16>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint4(r)) => { return Ok(compare_number::<Op, Decimal, u32>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint8(r)) => { return Ok(compare_number::<Op, Decimal, u64>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint16(r)) => { return Ok(compare_number::<Op, Decimal, u128>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Int>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Uint>(l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Decimal>(l, r, $fragment)); },

			// Additional arms
			$($extra)*
		}
	};
}

// Trait for comparison operations - monomorphized for fast execution
pub(crate) trait CompareOp {
	fn compare_ordering(ordering: Option<Ordering>) -> bool;
	fn compare_bool(_l: bool, _r: bool) -> Option<bool> {
		None
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
fn compare_number<Op: CompareOp, L, R>(l: &NumberContainer<L>, r: &NumberContainer<R>, fragment: Fragment) -> Column
where
	L: Promote<R> + IsNumber,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.data().iter()
			.zip(r.data().iter())
			.map(|(l_val, r_val)| Op::compare_ordering(partial_cmp(l_val, r_val)))
			.collect();

	Column {
		name: Fragment::internal(fragment.text()),
		data: ColumnData::bool(data),
	}
}

#[inline]
fn compare_temporal<Op: CompareOp, T>(l: &TemporalContainer<T>, r: &TemporalContainer<T>, fragment: Fragment) -> Column
where
	T: IsTemporal + Copy + PartialOrd,
{
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.data().iter()
			.zip(r.data().iter())
			.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
			.collect();

	Column {
		name: Fragment::internal(fragment.text()),
		data: ColumnData::bool(data),
	}
}

#[inline]
fn compare_uuid<Op: CompareOp, T>(l: &UuidContainer<T>, r: &UuidContainer<T>, fragment: Fragment) -> Column
where
	T: IsUuid + PartialOrd,
{
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.data().iter()
			.zip(r.data().iter())
			.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
			.collect();

	Column {
		name: Fragment::internal(fragment.text()),
		data: ColumnData::bool(data),
	}
}

#[inline]
fn compare_blob<Op: CompareOp>(l: &BlobContainer, r: &BlobContainer, fragment: Fragment) -> Column {
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.data().iter()
			.zip(r.data().iter())
			.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
			.collect();

	Column {
		name: Fragment::internal(fragment.text()),
		data: ColumnData::bool(data),
	}
}

#[inline]
fn compare_utf8<Op: CompareOp>(l: &Utf8Container, r: &Utf8Container, fragment: Fragment) -> Column {
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.data().iter()
			.zip(r.data().iter())
			.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
			.collect();

	Column {
		name: Fragment::internal(fragment.text()),
		data: ColumnData::bool(data),
	}
}

#[inline]
fn compare_bool<Op: CompareOp>(l: &BoolContainer, r: &BoolContainer, fragment: Fragment) -> Option<Column> {
	debug_assert_eq!(l.len(), r.len());

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
}

pub(crate) fn compare_columns<Op: CompareOp>(
	left: &Column,
	right: &Column,
	fragment: Fragment,
	error_fn: impl FnOnce(Fragment, Type, Type) -> Diagnostic,
) -> crate::Result<Column> {
	super::option::binary_op_unwrap_option(left, right, fragment.clone(), |left, right| {
		dispatch_compare!(
			&left.data(), &right.data();
			fragment;

			(ColumnData::Bool(l), ColumnData::Bool(r)) => {
				if let Some(col) = compare_bool::<Op>(l, r, fragment.clone()) {
					return Ok(col);
				}
				return_error!(error_fn(fragment, left.get_type(), right.get_type()))
			}

			(ColumnData::Date(l), ColumnData::Date(r)) => {
				return Ok(compare_temporal::<Op, _>(l, r, fragment));
			},
			(ColumnData::DateTime(l), ColumnData::DateTime(r)) => {
				return Ok(compare_temporal::<Op, _>(l, r, fragment));
			},
			(ColumnData::Time(l), ColumnData::Time(r)) => {
				return Ok(compare_temporal::<Op, _>(l, r, fragment));
			},
			(ColumnData::Duration(l), ColumnData::Duration(r)) => {
				return Ok(compare_temporal::<Op, _>(l, r, fragment));
			},

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
			},

			(ColumnData::Uuid4(l), ColumnData::Uuid4(r)) => {
				return Ok(compare_uuid::<Op, _>(l, r, fragment));
			},
			(ColumnData::Uuid7(l), ColumnData::Uuid7(r)) => {
				return Ok(compare_uuid::<Op, _>(l, r, fragment));
			},
			(
				ColumnData::Blob {
					container: l,
					..
				},
				ColumnData::Blob {
					container: r,
					..
				},
			) => {
				return Ok(compare_blob::<Op>(l, r, fragment));
			},

			_ => {
				return_error!(error_fn(fragment, left.get_type(), right.get_type()))
			},
		)
	})
}
