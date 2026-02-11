// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cmp::Ordering;

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{
	error::diagnostic::Diagnostic,
	fragment::Fragment,
	return_error,
	value::{
		container::{
			bool::BoolContainer, number::NumberContainer, temporal::TemporalContainer,
			undefined::UndefinedContainer, utf8::Utf8Container,
		},
		decimal::Decimal,
		int::Int,
		is::{IsNumber, IsTemporal},
		number::{compare::partial_cmp, promote::Promote},
		r#type::{Type, Type::Boolean},
		uint::Uint,
	},
};

use crate::expression::context::EvalContext;

/// Generates a complete match expression dispatching all numeric type pairs for comparison.
/// Uses push-down accumulation to build the cross-product of type arms.
macro_rules! dispatch_compare {
	// Entry point
	(
		$left:expr, $right:expr;
		$ctx:expr, $fragment:expr;
		$($extra:tt)*
	) => {
		dispatch_compare!(@rows
			($left, $right) ($ctx, $fragment)
			[(Float4, f32) (Float8, f64) (Int1, i8) (Int2, i16) (Int4, i32) (Int8, i64) (Int16, i128) (Uint1, u8) (Uint2, u16) (Uint4, u32) (Uint8, u64) (Uint16, u128)]
			{$($extra)*}
			{}
		)
	};

	// Recursive: process one fixed-left type pair, generating all 15 right-side arms
	(@rows
		($left:expr, $right:expr) ($ctx:expr, $fragment:expr)
		[($L:ident, $Lt:ty) $($rest:tt)*]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		dispatch_compare!(@rows
			($left, $right) ($ctx, $fragment)
			[$($rest)*]
			{$($extra)*}
			{
				$($acc)*
				(ColumnData::$L(l), ColumnData::Float4(r)) => { return Ok(compare_number::<Op, $Lt, f32>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Float8(r)) => { return Ok(compare_number::<Op, $Lt, f64>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int1(r)) => { return Ok(compare_number::<Op, $Lt, i8>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int2(r)) => { return Ok(compare_number::<Op, $Lt, i16>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int4(r)) => { return Ok(compare_number::<Op, $Lt, i32>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int8(r)) => { return Ok(compare_number::<Op, $Lt, i64>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int16(r)) => { return Ok(compare_number::<Op, $Lt, i128>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint1(r)) => { return Ok(compare_number::<Op, $Lt, u8>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint2(r)) => { return Ok(compare_number::<Op, $Lt, u16>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint4(r)) => { return Ok(compare_number::<Op, $Lt, u32>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint8(r)) => { return Ok(compare_number::<Op, $Lt, u64>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint16(r)) => { return Ok(compare_number::<Op, $Lt, u128>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Int { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Int>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Uint { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Uint>($ctx, l, r, $fragment)); },
				(ColumnData::$L(l), ColumnData::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, $Lt, Decimal>($ctx, l, r, $fragment)); },
			}
		)
	};

	// Base case: all fixed-left types processed, emit the match with arb-left arms
	(@rows
		($left:expr, $right:expr) ($ctx:expr, $fragment:expr)
		[]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		match ($left, $right) {
			// Fixed × all (12 × 15 = 180 arms)
			$($acc)*

			// Int × all (15 arms)
			(ColumnData::Int { container: l, .. }, ColumnData::Float4(r)) => { return Ok(compare_number::<Op, Int, f32>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Float8(r)) => { return Ok(compare_number::<Op, Int, f64>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int1(r)) => { return Ok(compare_number::<Op, Int, i8>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int2(r)) => { return Ok(compare_number::<Op, Int, i16>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int4(r)) => { return Ok(compare_number::<Op, Int, i32>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int8(r)) => { return Ok(compare_number::<Op, Int, i64>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int16(r)) => { return Ok(compare_number::<Op, Int, i128>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint1(r)) => { return Ok(compare_number::<Op, Int, u8>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint2(r)) => { return Ok(compare_number::<Op, Int, u16>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint4(r)) => { return Ok(compare_number::<Op, Int, u32>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint8(r)) => { return Ok(compare_number::<Op, Int, u64>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint16(r)) => { return Ok(compare_number::<Op, Int, u128>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Int { container: r, .. }) => { return Ok(compare_number::<Op, Int, Int>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Int, Uint>($ctx, l, r, $fragment)); },
			(ColumnData::Int { container: l, .. }, ColumnData::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Int, Decimal>($ctx, l, r, $fragment)); },

			// Uint × all (15 arms)
			(ColumnData::Uint { container: l, .. }, ColumnData::Float4(r)) => { return Ok(compare_number::<Op, Uint, f32>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Float8(r)) => { return Ok(compare_number::<Op, Uint, f64>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int1(r)) => { return Ok(compare_number::<Op, Uint, i8>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int2(r)) => { return Ok(compare_number::<Op, Uint, i16>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int4(r)) => { return Ok(compare_number::<Op, Uint, i32>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int8(r)) => { return Ok(compare_number::<Op, Uint, i64>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int16(r)) => { return Ok(compare_number::<Op, Uint, i128>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint1(r)) => { return Ok(compare_number::<Op, Uint, u8>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint2(r)) => { return Ok(compare_number::<Op, Uint, u16>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint4(r)) => { return Ok(compare_number::<Op, Uint, u32>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint8(r)) => { return Ok(compare_number::<Op, Uint, u64>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint16(r)) => { return Ok(compare_number::<Op, Uint, u128>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Int { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Int>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Uint>($ctx, l, r, $fragment)); },
			(ColumnData::Uint { container: l, .. }, ColumnData::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Uint, Decimal>($ctx, l, r, $fragment)); },

			// Decimal × all (15 arms)
			(ColumnData::Decimal { container: l, .. }, ColumnData::Float4(r)) => { return Ok(compare_number::<Op, Decimal, f32>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Float8(r)) => { return Ok(compare_number::<Op, Decimal, f64>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int1(r)) => { return Ok(compare_number::<Op, Decimal, i8>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int2(r)) => { return Ok(compare_number::<Op, Decimal, i16>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int4(r)) => { return Ok(compare_number::<Op, Decimal, i32>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int8(r)) => { return Ok(compare_number::<Op, Decimal, i64>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int16(r)) => { return Ok(compare_number::<Op, Decimal, i128>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint1(r)) => { return Ok(compare_number::<Op, Decimal, u8>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint2(r)) => { return Ok(compare_number::<Op, Decimal, u16>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint4(r)) => { return Ok(compare_number::<Op, Decimal, u32>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint8(r)) => { return Ok(compare_number::<Op, Decimal, u64>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint16(r)) => { return Ok(compare_number::<Op, Decimal, u128>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Int>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Uint>($ctx, l, r, $fragment)); },
			(ColumnData::Decimal { container: l, .. }, ColumnData::Decimal { container: r, .. }) => { return Ok(compare_number::<Op, Decimal, Decimal>($ctx, l, r, $fragment)); },

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
	ctx: &EvalContext,
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
				.map(|(l_val, r_val)| Op::compare_ordering(partial_cmp(l_val, r_val)))
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
					data.push(Op::compare_ordering(partial_cmp(l, r)));
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
					data.push(false);
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
					data.push(false);
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
	ctx: &EvalContext,
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
	ctx: &EvalContext,
	left: &Column,
	right: &Column,
	fragment: Fragment,
	error_fn: impl FnOnce(Fragment, Type, Type) -> Diagnostic,
) -> crate::Result<Column> {
	dispatch_compare!(
		&left.data(), &right.data();
		ctx, fragment;

		(ColumnData::Bool(l), ColumnData::Bool(r)) => {
			if let Some(col) = compare_bool::<Op>(ctx, l, r, fragment.clone()) {
				return Ok(col);
			}
			return_error!(error_fn(fragment, left.get_type(), right.get_type()))
		}

		// Temporal types
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
		},
		// Undefined
		(ColumnData::Undefined(container), _) | (_, ColumnData::Undefined(container)) => {
			return Ok(Column {
				name: Fragment::internal(fragment.text()),
				data: ColumnData::Undefined(UndefinedContainer::new(container.len())),
			});
		},
		_ => {
			return_error!(error_fn(fragment, left.get_type(), right.get_type()))
		},
	)
}
