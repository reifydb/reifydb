// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData, push::Push};
use reifydb_type::{
	error::diagnostic::operator::mul_cannot_be_applied_to_incompatible_types,
	fragment::LazyFragment,
	return_error,
	value::{
		container::{number::NumberContainer, undefined::UndefinedContainer},
		is::IsNumber,
		number::{promote::Promote, safe::mul::SafeMul},
		r#type::{Type, get::GetType},
	},
};

use crate::expression::context::EvalContext;

pub(crate) fn mul_columns(
	ctx: &EvalContext,
	left: &Column,
	right: &Column,
	fragment: impl LazyFragment + Copy,
) -> crate::Result<Column> {
	let target = Type::promote(left.get_type(), right.get_type());

	dispatch_arith!(
		&left.data(), &right.data();
		fixed: mul_numeric, arb: mul_numeric_clone (ctx, target, fragment);

		// Handle undefined values - any operation with
		// undefined results in undefined
		(ColumnData::Undefined(l), _) => Ok(Column {
			name: fragment.fragment(),
			data: ColumnData::Undefined(UndefinedContainer::new(l.len())),
		}),
		(_, ColumnData::Undefined(r)) => Ok(Column {
			name: fragment.fragment(),
			data: ColumnData::Undefined(UndefinedContainer::new(r.len())),
		}),

		_ => return_error!(mul_cannot_be_applied_to_incompatible_types(
			fragment.fragment(),
			left.get_type(),
			right.get_type(),
		)),
	)
}

fn mul_numeric<'a, L, R>(
	ctx: &EvalContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment + Copy,
) -> crate::Result<Column>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeMul,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let mut data = ctx.pooled(target, l.len());
		let l_data = l.data();
		let r_data = r.data();

		for i in 0..l.len() {
			if let Some(value) = ctx.mul(&l_data[i], &r_data[i], fragment)? {
				data.push(value);
			} else {
				data.push_undefined()
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	} else {
		// Slow path: some values may be undefined
		let mut data = ctx.pooled(target, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					if let Some(value) = ctx.mul(l, r, fragment)? {
						data.push(value);
					} else {
						data.push_undefined()
					}
				}
				_ => data.push_undefined(),
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	}
}

fn mul_numeric_clone<'a, L, R>(
	ctx: &EvalContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment + Copy,
) -> crate::Result<Column>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeMul,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let mut data = ctx.pooled(target, l.len());
		let l_data = l.data();
		let r_data = r.data();

		for i in 0..l.len() {
			let l_clone = l_data[i].clone();
			let r_clone = r_data[i].clone();
			if let Some(value) = ctx.mul(&l_clone, &r_clone, fragment)? {
				data.push(value);
			} else {
				data.push_undefined()
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	} else {
		// Slow path: some values may be undefined
		let mut data = ctx.pooled(target, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l_val), Some(r_val)) => {
					let l_clone = l_val.clone();
					let r_clone = r_val.clone();
					if let Some(value) = ctx.mul(&l_clone, &r_clone, fragment)? {
						data.push(value);
					} else {
						data.push_undefined()
					}
				}
				_ => data.push_undefined(),
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	}
}
