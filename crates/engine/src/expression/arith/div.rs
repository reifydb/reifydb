// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData, push::Push};
use reifydb_type::{
	error::diagnostic::operator::div_cannot_be_applied_to_incompatible_types,
	fragment::LazyFragment,
	return_error,
	value::{
		container::number::NumberContainer,
		is::IsNumber,
		number::{promote::Promote, safe::div::SafeDiv},
		r#type::{Type, get::GetType},
	},
};

use crate::expression::context::EvalContext;

pub(crate) fn div_columns(
	ctx: &EvalContext,
	left: &Column,
	right: &Column,
	fragment: impl LazyFragment + Copy,
) -> crate::Result<Column> {
	crate::expression::option::binary_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let target = Type::promote(left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data(), &right.data();
			fixed: div_numeric, arb: div_numeric_clone (ctx, target, fragment);

			_ => return_error!(div_cannot_be_applied_to_incompatible_types(
				fragment.fragment(),
				left.get_type(),
				right.get_type(),
			)),
		)
	})
}

fn div_numeric<'a, L, R>(
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
	<L as Promote<R>>::Output: SafeDiv,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnData::with_capacity(target, l.len());
	let l_data = l.data();
	let r_data = r.data();
	for i in 0..l.len() {
		if let Some(value) = ctx.div(&l_data[i], &r_data[i], fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(Column {
		name: fragment.fragment(),
		data,
	})
}

fn div_numeric_clone<'a, L, R>(
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
	<L as Promote<R>>::Output: SafeDiv,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnData::with_capacity(target, l.len());
	let l_data = l.data();
	let r_data = r.data();
	for i in 0..l.len() {
		let l_clone = l_data[i].clone();
		let r_clone = r_data[i].clone();
		if let Some(value) = ctx.div(&l_clone, &r_clone, fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(Column {
		name: fragment.fragment(),
		data,
	})
}
