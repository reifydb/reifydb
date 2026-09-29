// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::{builder::ColumnBuilder, push::Push};
use reifydb_value::{
	error::{BinaryOp, TypeError},
	fragment::LazyFragment,
	reifydb_assertions,
	value::{
		column_view::{ColumnView, ViewData},
		container::{decimal_array::decimals, wide_int_array::wides},
		is::IsNumber,
		number::{promote::Promote, safe::div::SafeDiv},
		value_type::{ValueType, get::GetType},
	},
};

use crate::{
	Result,
	expression::{
		arith::{ArithOp, arith_target},
		context::EvalContext,
		option::arith_op_unwrap_option,
		scalar::FitFamily,
	},
};

pub fn div_columns(
	ctx: &EvalContext,
	left: &(FieldRef, ArrayRef),
	right: &(FieldRef, ArrayRef),
	fragment: impl LazyFragment + Copy,
) -> Result<(FieldRef, ArrayRef)> {
	arith_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let (left, right) = (ColumnView::try_from(left)?, ColumnView::try_from(right)?);
		let target = arith_target(ArithOp::Div, left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data, &right.data;
			fixed: div_numeric, arb: div_numeric_clone (ctx, target, fragment);

			_ => Err(TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Div,
				left: left.get_type(),
				right: right.get_type(),
				fragment: fragment.fragment(),
			}.into()),
		)
	})
}

fn div_numeric<L, R>(
	ctx: &EvalContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<(FieldRef, ArrayRef)>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeDiv,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target, l.len());
	for i in 0..l.len() {
		if let Some(value) = ctx.div(&l[i], &r[i], fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(data.finish(fragment.fragment().text()))
}

fn div_numeric_clone<L, R>(
	ctx: &EvalContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<(FieldRef, ArrayRef)>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber + FitFamily,
	<L as Promote<R>>::Output: SafeDiv,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target.clone(), l.len());
	for i in 0..l.len() {
		let l_clone = l[i].clone();
		let r_clone = r[i].clone();
		match ctx.div(&l_clone, &r_clone, fragment)? {
			Some(value) => match ctx.fit_family(value, &target, fragment)? {
				Some(value) => data.push(value),
				None => data.push_none(),
			},
			None => data.push_none(),
		}
	}
	Ok(data.finish(fragment.fragment().text()))
}
