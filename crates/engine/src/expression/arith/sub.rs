// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData, push::Push};
use reifydb_type::{
	error::{BinaryOp, TypeError},
	fragment::LazyFragment,
	value::{
		container::{number::NumberContainer, temporal::TemporalContainer},
		is::IsNumber,
		number::{promote::Promote, safe::sub::SafeSub},
		r#type::{Type, get::GetType},
	},
};

use crate::expression::context::EvalContext;

pub(crate) fn sub_columns(
	ctx: &EvalContext,
	left: &Column,
	right: &Column,
	fragment: impl LazyFragment + Copy,
) -> crate::Result<Column> {
	crate::expression::option::binary_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let target = Type::promote(left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data(), &right.data();
			fixed: sub_numeric, arb: sub_numeric_clone (ctx, target, fragment);

			// Duration - Duration
			(ColumnData::Duration(l), ColumnData::Duration(r)) => {
				let mut container = TemporalContainer::with_capacity(l.len());
				for i in 0..l.len() {
					match (l.get(i), r.get(i)) {
						(Some(lv), Some(rv)) => container.push(*lv - *rv),
						_ => container.push_default(),
					}
				}
				Ok(Column {
					name: fragment.fragment(),
					data: ColumnData::Duration(container),
				})
			}

			_ => return Err(TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Sub,
				left: left.get_type(),
				right: right.get_type(),
				fragment: fragment.fragment(),
			}.into()),
		)
	})
}

fn sub_numeric<'a, L, R>(
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
	<L as Promote<R>>::Output: SafeSub,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnData::with_capacity(target, l.len());
	let l_data = l.data();
	let r_data = r.data();
	for i in 0..l.len() {
		if let Some(value) = ctx.sub(&l_data[i], &r_data[i], fragment)? {
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

fn sub_numeric_clone<'a, L, R>(
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
	<L as Promote<R>>::Output: SafeSub,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnData::with_capacity(target, l.len());
	let l_data = l.data();
	let r_data = r.data();
	for i in 0..l.len() {
		let l_clone = l_data[i].clone();
		let r_clone = r_data[i].clone();
		if let Some(value) = ctx.sub(&l_clone, &r_clone, fragment)? {
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
