// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder, push::Push};
use reifydb_value::{
	error::{BinaryOp, TypeError},
	fragment::LazyFragment,
	reifydb_assertions,
	value::{
		container::{
			bignum_array::{decimals, ints, uints},
			decimal_array::u128s,
			temporal_array::{duration_array, durations},
		},
		is::IsNumber,
		number::{promote::Promote, safe::sub::SafeSub},
		value_type::{ValueType, get::GetType},
	},
};

use crate::{
	Result,
	expression::{compare::length_mismatch, context::EvalContext, option::arith_op_unwrap_option},
};

pub fn sub_columns(
	ctx: &EvalContext,
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName> {
	arith_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let target = ValueType::promote(left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data(), &right.data();
			fixed: sub_numeric, arb: sub_numeric_clone (ctx, target, fragment);


			(ColumnBuffer::Duration(l), ColumnBuffer::Duration(r)) => {
				let (l, r) = (durations(l), durations(r));
				let values = (0..l.len())
					.map(|i| match (l.get(i), r.get(i)) {
						(Some(lv), Some(rv)) => lv.try_sub(*rv).map_err(|e| (*e).into()),
						_ => Err(length_mismatch(l.len(), r.len(), &fragment.fragment())),
					})
					.collect::<Result<Vec<_>>>()?;
				Ok(ColumnWithName::new(fragment.fragment(), ColumnBuffer::Duration(duration_array(values))))
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
	ctx: &EvalContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeSub,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target, l.len());
	for i in 0..l.len() {
		if let Some(value) = ctx.sub(&l[i], &r[i], fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(ColumnWithName {
		name: fragment.fragment(),
		data: data.finish(),
	})
}

fn sub_numeric_clone<L, R>(
	ctx: &EvalContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeSub,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target, l.len());
	for i in 0..l.len() {
		let l_clone = l[i].clone();
		let r_clone = r[i].clone();
		if let Some(value) = ctx.sub(&l_clone, &r_clone, fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(ColumnWithName {
		name: fragment.fragment(),
		data: data.finish(),
	})
}
