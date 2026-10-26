// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	error::{BinaryOp, TypeError},
	fragment::LazyFragment,
	reifydb_assertions,
	value::{
		column_view::{ColumnView, ViewData},
		container::{decimal_array::decimals, temporal_array::durations, wide_int_array::wides},
		is::IsNumber,
		number::{promote::Promote, safe::sub::SafeSub},
		value_type::{ValueType, get::GetType},
	},
};

use crate::value::column::{
	arith::{
		ArithOp, arith_target, context::ArithContext, length_mismatch, option::arith_op_unwrap_option,
		output_len, row_of, scalar::FitFamily,
	},
	builder::ColumnBuilder,
	factory,
	push::Push,
};

pub fn sub_columns(
	ctx: &ArithContext,
	left: &(FieldRef, ArrayRef),
	right: &(FieldRef, ArrayRef),
	fragment: impl LazyFragment + Copy,
) -> Result<(FieldRef, ArrayRef)> {
	arith_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let (left, right) = (ColumnView::try_from(left)?, ColumnView::try_from(right)?);
		let target = arith_target(ArithOp::Sub, left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data, &right.data;
			fixed: sub_numeric, arb: sub_numeric_clone (ctx, target, fragment);


			(ViewData::Duration(l), ViewData::Duration(r)) => {
				let (l, r) = (durations(l), durations(r));
				let values = (0..output_len(l.len(), r.len()))
					.map(|i| match (l.get(row_of(l.len(), i)), r.get(row_of(r.len(), i))) {
						(Some(lv), Some(rv)) => lv.try_sub(*rv).map_err(|e| (*e).into()),
						_ => Err(length_mismatch(l.len(), r.len(), &fragment.fragment())),
					})
					.collect::<Result<Vec<_>>>()?;
				Ok(factory::duration(fragment.fragment().text(), values))
			}

			_ => Err(TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Sub,
				left: left.get_type(),
				right: right.get_type(),
				fragment: fragment.fragment(),
			}.into()),
		)
	})
}

fn sub_numeric<L, R>(
	ctx: &ArithContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<(FieldRef, ArrayRef)>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeSub,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert!(l.len() == r.len() || l.len() == 1 || r.len() == 1);
	}

	let mut data = ColumnBuilder::with_capacity(target, output_len(l.len(), r.len()));
	for i in 0..output_len(l.len(), r.len()) {
		if let Some(value) = ctx.sub(&l[row_of(l.len(), i)], &r[row_of(r.len(), i)], fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(data.finish(fragment.fragment().text()))
}

fn sub_numeric_clone<L, R>(
	ctx: &ArithContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<(FieldRef, ArrayRef)>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber + FitFamily,
	<L as Promote<R>>::Output: SafeSub,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert!(l.len() == r.len() || l.len() == 1 || r.len() == 1);
	}

	let mut data = ColumnBuilder::with_capacity(target.clone(), output_len(l.len(), r.len()));
	for i in 0..output_len(l.len(), r.len()) {
		let l_clone = l[row_of(l.len(), i)].clone();
		let r_clone = r[row_of(r.len(), i)].clone();
		match ctx.sub(&l_clone, &r_clone, fragment)? {
			Some(value) => match ctx.fit_family(value, &target, fragment)? {
				Some(value) => data.push(value),
				None => data.push_none(),
			},
			None => data.push_none(),
		}
	}
	Ok(data.finish(fragment.fragment().text()))
}
