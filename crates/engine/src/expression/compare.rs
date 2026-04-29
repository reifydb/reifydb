// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::cmp::Ordering;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_type::{
	error::Diagnostic,
	fragment::Fragment,
	return_error,
	value::{
		container::{
			blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container, uuid::UuidContainer,
		},
		decimal::Decimal,
		int::Int,
		is::{IsNumber, IsTemporal, IsUuid},
		number::{compare::partial_cmp, promote::Promote},
		r#type::Type,
		uint::Uint,
	},
};

use super::option::binary_op_unwrap_option;
use crate::Result;

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
				(ColumnBuffer::$L(l), ColumnBuffer::Float4(r)) => { return Ok(compare_number::<Op, $Lt, f32>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Float8(r)) => { return Ok(compare_number::<Op, $Lt, f64>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int1(r)) => { return Ok(compare_number::<Op, $Lt, i8>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int2(r)) => { return Ok(compare_number::<Op, $Lt, i16>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int4(r)) => { return Ok(compare_number::<Op, $Lt, i32>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int8(r)) => { return Ok(compare_number::<Op, $Lt, i64>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int16(r)) => { return Ok(compare_number::<Op, $Lt, i128>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint1(r)) => { return Ok(compare_number::<Op, $Lt, u8>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint2(r)) => { return Ok(compare_number::<Op, $Lt, u16>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint4(r)) => { return Ok(compare_number::<Op, $Lt, u32>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint8(r)) => { return Ok(compare_number::<Op, $Lt, u64>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint16(r)) => { return Ok(compare_number::<Op, $Lt, u128>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Int { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Int>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Uint { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Uint>(l, r, $fragment)); },
				(ColumnBuffer::$L(l), ColumnBuffer::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Decimal>(l, r, $fragment)); },
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
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Float4(r)) => { return Ok(compare_number::<Op, Int, f32>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Float8(r)) => { return Ok(compare_number::<Op, Int, f64>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int1(r)) => { return Ok(compare_number::<Op, Int, i8>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int2(r)) => { return Ok(compare_number::<Op, Int, i16>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int4(r)) => { return Ok(compare_number::<Op, Int, i32>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int8(r)) => { return Ok(compare_number::<Op, Int, i64>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int16(r)) => { return Ok(compare_number::<Op, Int, i128>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint1(r)) => { return Ok(compare_number::<Op, Int, u8>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint2(r)) => { return Ok(compare_number::<Op, Int, u16>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint4(r)) => { return Ok(compare_number::<Op, Int, u32>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint8(r)) => { return Ok(compare_number::<Op, Int, u64>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint16(r)) => { return Ok(compare_number::<Op, Int, u128>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => { return Ok(compare_number::<Op, Int, Int>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Int, Uint>(l, r, $fragment)); },
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Int, Decimal>(l, r, $fragment)); },

			// Uint × all (15 arms)
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Float4(r)) => { return Ok(compare_number::<Op, Uint, f32>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Float8(r)) => { return Ok(compare_number::<Op, Uint, f64>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int1(r)) => { return Ok(compare_number::<Op, Uint, i8>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int2(r)) => { return Ok(compare_number::<Op, Uint, i16>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int4(r)) => { return Ok(compare_number::<Op, Uint, i32>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int8(r)) => { return Ok(compare_number::<Op, Uint, i64>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int16(r)) => { return Ok(compare_number::<Op, Uint, i128>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint1(r)) => { return Ok(compare_number::<Op, Uint, u8>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint2(r)) => { return Ok(compare_number::<Op, Uint, u16>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint4(r)) => { return Ok(compare_number::<Op, Uint, u32>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint8(r)) => { return Ok(compare_number::<Op, Uint, u64>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint16(r)) => { return Ok(compare_number::<Op, Uint, u128>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Int>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Uint>(l, r, $fragment)); },
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Decimal>(l, r, $fragment)); },

			// Decimal × all (15 arms)
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Float4(r)) => { return Ok(compare_number::<Op, Decimal, f32>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Float8(r)) => { return Ok(compare_number::<Op, Decimal, f64>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int1(r)) => { return Ok(compare_number::<Op, Decimal, i8>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int2(r)) => { return Ok(compare_number::<Op, Decimal, i16>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int4(r)) => { return Ok(compare_number::<Op, Decimal, i32>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int8(r)) => { return Ok(compare_number::<Op, Decimal, i64>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int16(r)) => { return Ok(compare_number::<Op, Decimal, i128>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint1(r)) => { return Ok(compare_number::<Op, Decimal, u8>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint2(r)) => { return Ok(compare_number::<Op, Decimal, u16>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint4(r)) => { return Ok(compare_number::<Op, Decimal, u32>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint8(r)) => { return Ok(compare_number::<Op, Decimal, u64>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint16(r)) => { return Ok(compare_number::<Op, Decimal, u128>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Int>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Uint>(l, r, $fragment)); },
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Decimal>(l, r, $fragment)); },

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
fn compare_number<Op: CompareOp, L, R>(
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	fragment: Fragment,
) -> ColumnWithName
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

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_temporal<Op: CompareOp, T>(
	l: &TemporalContainer<T>,
	r: &TemporalContainer<T>,
	fragment: Fragment,
) -> ColumnWithName
where
	T: IsTemporal + Copy + PartialOrd,
{
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.data().iter()
			.zip(r.data().iter())
			.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
			.collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_uuid<Op: CompareOp, T>(l: &UuidContainer<T>, r: &UuidContainer<T>, fragment: Fragment) -> ColumnWithName
where
	T: IsUuid + PartialOrd,
{
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.data().iter()
			.zip(r.data().iter())
			.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
			.collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_identity_id<Op: CompareOp>(
	l: &IdentityIdContainer,
	r: &IdentityIdContainer,
	fragment: Fragment,
) -> ColumnWithName {
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.iter().zip(r.iter()).map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(&r_val))).collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_blob<Op: CompareOp>(l: &BlobContainer, r: &BlobContainer, fragment: Fragment) -> ColumnWithName {
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> = l
		.iter_bytes()
		.zip(r.iter_bytes())
		.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
		.collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_utf8<Op: CompareOp>(l: &Utf8Container, r: &Utf8Container, fragment: Fragment) -> ColumnWithName {
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> = l
		.iter_str()
		.zip(r.iter_str())
		.map(|(l_val, r_val)| Op::compare_ordering(l_val.partial_cmp(r_val)))
		.collect();

	ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data))
}

#[inline]
fn compare_bool<Op: CompareOp>(l: &BoolContainer, r: &BoolContainer, fragment: Fragment) -> Option<ColumnWithName> {
	debug_assert_eq!(l.len(), r.len());

	let data: Vec<bool> =
		l.data().iter()
			.zip(r.data().iter())
			.filter_map(|(l_val, r_val)| Op::compare_bool(l_val, r_val))
			.collect();

	if data.len() == l.len() {
		Some(ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::bool(data)))
	} else {
		None
	}
}

pub(crate) fn compare_columns<Op: CompareOp>(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: Fragment,
	error_fn: impl FnOnce(Fragment, Type, Type) -> Diagnostic,
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
				Ok(compare_temporal::<Op, _>(l, r, fragment))
			},
			(ColumnBuffer::DateTime(l), ColumnBuffer::DateTime(r)) => {
				Ok(compare_temporal::<Op, _>(l, r, fragment))
			},
			(ColumnBuffer::Time(l), ColumnBuffer::Time(r)) => {
				Ok(compare_temporal::<Op, _>(l, r, fragment))
			},
			(ColumnBuffer::Duration(l), ColumnBuffer::Duration(r)) => {
				Ok(compare_temporal::<Op, _>(l, r, fragment))
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
				Ok(compare_uuid::<Op, _>(l, r, fragment))
			},
			(ColumnBuffer::Uuid7(l), ColumnBuffer::Uuid7(r)) => {
				Ok(compare_uuid::<Op, _>(l, r, fragment))
			},
			(ColumnBuffer::IdentityId(l), ColumnBuffer::IdentityId(r)) => {
				Ok(compare_identity_id::<Op>(l, r, fragment))
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
