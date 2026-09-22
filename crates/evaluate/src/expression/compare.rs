// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Ordering;

use arrow_array::{Array, BooleanArray, LargeBinaryArray, LargeStringArray};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_value::{
	error::Diagnostic,
	fragment::Fragment,
	reifydb_assertions, return_error,
	value::{
		container::{
			bignum_array::{decimals, ints, uints},
			decimal_array::u128s,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
		},
		decimal::Decimal,
		identity::IdentityId,
		int::Int,
		is::{IsNumber, IsTemporal, IsUuid},
		number::{compare::partial_cmp, promote::Promote},
		uint::Uint,
		value_type::ValueType,
	},
};

use super::option::binary_op_unwrap_option;
use crate::Result;

macro_rules! dispatch_compare {

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
				(ColumnBuffer::$L(l), ColumnBuffer::Float4(r)) => { return Ok(compare_number::<Op, $Lt, f32>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Float8(r)) => { return Ok(compare_number::<Op, $Lt, f64>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int1(r)) => { return Ok(compare_number::<Op, $Lt, i8>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int2(r)) => { return Ok(compare_number::<Op, $Lt, i16>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int4(r)) => { return Ok(compare_number::<Op, $Lt, i32>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int8(r)) => { return Ok(compare_number::<Op, $Lt, i64>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int16(r)) => { return Ok(compare_number::<Op, $Lt, i128>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint1(r)) => { return Ok(compare_number::<Op, $Lt, u8>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint2(r)) => { return Ok(compare_number::<Op, $Lt, u16>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint4(r)) => { return Ok(compare_number::<Op, $Lt, u32>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint8(r)) => { return Ok(compare_number::<Op, $Lt, u64>(dispatch_compare!(@values $L l), r.values(), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint16(r)) => { return Ok(compare_number::<Op, $Lt, u128>(dispatch_compare!(@values $L l), &u128s(r), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Int>(dispatch_compare!(@values $L l), &ints(r), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Uint>(dispatch_compare!(@values $L l), &uints(r), $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Decimal>(dispatch_compare!(@values $L l), &decimals(r), $fragment)); },
			}
		)
	};


	(@rows
		($left:expr, $right:expr) ($fragment:expr)
		[]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		match ($left, $right) {

			$($acc)*


			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Float4(r)) => { return Ok(compare_number::<Op, Int, f32>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Float8(r)) => { return Ok(compare_number::<Op, Int, f64>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int1(r)) => { return Ok(compare_number::<Op, Int, i8>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int2(r)) => { return Ok(compare_number::<Op, Int, i16>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int4(r)) => { return Ok(compare_number::<Op, Int, i32>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int8(r)) => { return Ok(compare_number::<Op, Int, i64>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int16(r)) => { return Ok(compare_number::<Op, Int, i128>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint1(r)) => { return Ok(compare_number::<Op, Int, u8>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint2(r)) => { return Ok(compare_number::<Op, Int, u16>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint4(r)) => { return Ok(compare_number::<Op, Int, u32>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint8(r)) => { return Ok(compare_number::<Op, Int, u64>(&ints(l), r.values(), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint16(r)) => { return Ok(compare_number::<Op, Int, u128>(&ints(l), &u128s(r), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => { return Ok(compare_number::<Op, Int, Int>(&ints(l), &ints(r), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Int, Uint>(&ints(l), &uints(r), $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Int, Decimal>(&ints(l), &decimals(r), $fragment)); },


			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Float4(r)) => { return Ok(compare_number::<Op, Uint, f32>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Float8(r)) => { return Ok(compare_number::<Op, Uint, f64>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int1(r)) => { return Ok(compare_number::<Op, Uint, i8>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int2(r)) => { return Ok(compare_number::<Op, Uint, i16>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int4(r)) => { return Ok(compare_number::<Op, Uint, i32>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int8(r)) => { return Ok(compare_number::<Op, Uint, i64>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int16(r)) => { return Ok(compare_number::<Op, Uint, i128>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint1(r)) => { return Ok(compare_number::<Op, Uint, u8>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint2(r)) => { return Ok(compare_number::<Op, Uint, u16>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint4(r)) => { return Ok(compare_number::<Op, Uint, u32>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint8(r)) => { return Ok(compare_number::<Op, Uint, u64>(&uints(l), r.values(), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint16(r)) => { return Ok(compare_number::<Op, Uint, u128>(&uints(l), &u128s(r), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Int>(&uints(l), &ints(r), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Uint>(&uints(l), &uints(r), $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Decimal>(&uints(l), &decimals(r), $fragment)); },


			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Float4(r)) => { return Ok(compare_number::<Op, Decimal, f32>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Float8(r)) => { return Ok(compare_number::<Op, Decimal, f64>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int1(r)) => { return Ok(compare_number::<Op, Decimal, i8>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int2(r)) => { return Ok(compare_number::<Op, Decimal, i16>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int4(r)) => { return Ok(compare_number::<Op, Decimal, i32>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int8(r)) => { return Ok(compare_number::<Op, Decimal, i64>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int16(r)) => { return Ok(compare_number::<Op, Decimal, i128>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint1(r)) => { return Ok(compare_number::<Op, Decimal, u8>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint2(r)) => { return Ok(compare_number::<Op, Decimal, u16>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint4(r)) => { return Ok(compare_number::<Op, Decimal, u32>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint8(r)) => { return Ok(compare_number::<Op, Decimal, u64>(&decimals(l), r.values(), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint16(r)) => { return Ok(compare_number::<Op, Decimal, u128>(&decimals(l), &u128s(r), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Int>(&decimals(l), &ints(r), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Uint>(&decimals(l), &uints(r), $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Decimal>(&decimals(l), &decimals(r), $fragment)); },


			$($extra)*
		}
	};


	(@values Uint16 $array:ident) => {
		&u128s($array)
	};


	(@values $L:ident $array:ident) => {
		$array.values()
	};
}

pub trait CompareOp {
	fn compare_ordering(ordering: Option<Ordering>) -> bool;
	fn compare_bool(_l: bool, _r: bool) -> Option<bool> {
		None
	}
}

pub struct Equal;
pub struct NotEqual;
pub struct GreaterThan;
pub struct GreaterThanEqual;
pub struct LessThan;
pub struct LessThanEqual;

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
fn compare_number<Op: CompareOp, L, R>(l: &[L], r: &[R], fragment: Fragment) -> ColumnWithName
where
	L: Promote<R> + IsNumber,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let data: Vec<bool> =
		l.iter().zip(r.iter()).map(|(l_val, r_val)| Op::compare_ordering(partial_cmp(l_val, r_val))).collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_temporal<Op: CompareOp, T>(l: &[T], r: &[T], fragment: Fragment) -> ColumnWithName
where
	T: IsTemporal + Copy + PartialOrd,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let data: Vec<bool> =
		l.iter().zip(r.iter()).map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val))).collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_uuid<Op: CompareOp, T>(l: &[T], r: &[T], fragment: Fragment) -> ColumnWithName
where
	T: IsUuid + PartialOrd,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let data: Vec<bool> =
		l.iter().zip(r.iter()).map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val))).collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_identity_id<Op: CompareOp>(l: &[IdentityId], r: &[IdentityId], fragment: Fragment) -> ColumnWithName {
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let data: Vec<bool> =
		l.iter().zip(r.iter()).map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val))).collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_blob<Op: CompareOp>(l: &LargeBinaryArray, r: &LargeBinaryArray, fragment: Fragment) -> ColumnWithName {
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let data: Vec<bool> = (0..l.len())
		.map(|i| l.value(i))
		.zip((0..r.len()).map(|i| r.value(i)))
		.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
		.collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_utf8<Op: CompareOp>(l: &LargeStringArray, r: &LargeStringArray, fragment: Fragment) -> ColumnWithName {
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let data: Vec<bool> = (0..l.len())
		.map(|i| l.value(i))
		.zip((0..r.len()).map(|i| r.value(i)))
		.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
		.collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_bool<Op: CompareOp>(l: &BooleanArray, r: &BooleanArray, fragment: Fragment) -> Option<ColumnWithName> {
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let data: Vec<bool> = l
		.values()
		.iter()
		.zip(r.values().iter())
		.filter_map(|(l_val, r_val)| Op::compare_bool(l_val, r_val))
		.collect();

	if data.len() == l.len() {
		Some(ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data)))
	} else {
		None
	}
}

pub fn compare_columns<Op: CompareOp>(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: Fragment,
	error_fn: impl FnOnce(Fragment, ValueType, ValueType) -> Diagnostic,
) -> Result<ColumnWithName> {
	binary_op_unwrap_option(left, right, fragment.clone(), |left, right| {
		dispatch_compare!(
			&left.data(), &right.data();
			fragment;

			(ColumnBuffer::Bool(l), ColumnBuffer::Bool(r)) => {
				if let Some(col) = compare_bool::<Op>(l, r, fragment.clone()) {
					return Ok(col);
				}
				return_error!(error_fn(fragment, left.get_type(), right.get_type()))
			}

			(ColumnBuffer::Date(l), ColumnBuffer::Date(r)) => {
				Ok(compare_temporal::<Op, _>(dates(l), dates(r), fragment))
			},
			(ColumnBuffer::DateTime(l), ColumnBuffer::DateTime(r)) => {
				Ok(compare_temporal::<Op, _>(datetimes(l), datetimes(r), fragment))
			},
			(ColumnBuffer::Time(l), ColumnBuffer::Time(r)) => {
				Ok(compare_temporal::<Op, _>(times(l), times(r), fragment))
			},
			(ColumnBuffer::Duration(l), ColumnBuffer::Duration(r)) => {
				Ok(compare_temporal::<Op, _>(durations(l), durations(r), fragment))
			},

			(
				ColumnBuffer::Utf8 {
					container: l,
					..
				},
				ColumnBuffer::Utf8 {
					container: r,
					..
				},
			) => {
				Ok(compare_utf8::<Op>(l, r, fragment))
			},

			(ColumnBuffer::Uuid4(l), ColumnBuffer::Uuid4(r)) => {
				Ok(compare_uuid::<Op, _>(uuid4s(l), uuid4s(r), fragment))
			},
			(ColumnBuffer::Uuid7(l), ColumnBuffer::Uuid7(r)) => {
				Ok(compare_uuid::<Op, _>(uuid7s(l), uuid7s(r), fragment))
			},
			(ColumnBuffer::IdentityId(l), ColumnBuffer::IdentityId(r)) => {
				Ok(compare_identity_id::<Op>(identity_ids(l), identity_ids(r), fragment))
			},
			(
				ColumnBuffer::Blob {
					container: l,
					..
				},
				ColumnBuffer::Blob {
					container: r,
					..
				},
			) => {
				Ok(compare_blob::<Op>(l, r, fragment))
			},

			_ => {
				return_error!(error_fn(fragment, left.get_type(), right.get_type()))
			},
		)
	})
}
