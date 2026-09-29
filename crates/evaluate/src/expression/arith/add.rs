// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_core::value::column::{builder::ColumnBuilder, factory, push::Push};
use reifydb_value::{
	error::{BinaryOp, TypeError},
	fragment::{Fragment, LazyFragment},
	reifydb_assertions,
	value::{
		column_view::{ColumnView, ViewData},
		container::{
			decimal_array::decimals, temporal_array::durations, varlen_array::get, wide_int_array::wides,
		},
		is::IsNumber,
		number::{promote::Promote, safe::add::SafeAdd},
		value_type::{ValueType, get::GetType},
	},
};

use crate::{
	Result,
	expression::{
		arith::{ArithOp, arith_target},
		compare::length_mismatch,
		context::EvalContext,
		option::arith_op_unwrap_option,
		scalar::FitFamily,
	},
};

pub fn add_columns(
	ctx: &EvalContext,
	left: &(FieldRef, ArrayRef),
	right: &(FieldRef, ArrayRef),
	fragment: impl LazyFragment + Copy,
) -> Result<(FieldRef, ArrayRef)> {
	arith_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let (left, right) = (ColumnView::try_from(left)?, ColumnView::try_from(right)?);
		let target = arith_target(ArithOp::Add, left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data, &right.data;
			fixed: add_numeric, arb: add_numeric_clone (ctx, target, fragment);


			(ViewData::Duration(l), ViewData::Duration(r)) => {
				let (l, r) = (durations(l), durations(r));
				let values = (0..l.len())
					.map(|i| match (l.get(i), r.get(i)) {
						(Some(lv), Some(rv)) => lv.try_add(*rv).map_err(|e| (*e).into()),
						_ => Err(length_mismatch(l.len(), r.len(), &fragment.fragment())),
					})
					.collect::<Result<Vec<_>>>()?;
				Ok(factory::duration(fragment.fragment().text(), values))
			}


			(
				ViewData::Utf8 {
					container: l,
					..
				},
				ViewData::Utf8 {
					container: r,
					..
				},
			) => concat_strings(l, r, target, fragment.fragment()),


			(
				ViewData::Utf8 {
					container: l,
					..
				},
				r,
			) if can_promote_to_string(r) => concat_string_with_other(l, &right, true, target, fragment.fragment()),


			(
				l,
				ViewData::Utf8 {
					container: r,
					..
				},
			) if can_promote_to_string(l) => concat_string_with_other(r, &left, false, target, fragment.fragment()),

			_ => Err(TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Add,
				left: left.get_type(),
				right: right.get_type(),
				fragment: fragment.fragment(),
			}.into()),
		)
	})
}

fn add_numeric<L, R>(
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
	<L as Promote<R>>::Output: SafeAdd,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target, l.len());
	for i in 0..l.len() {
		if let Some(value) = ctx.add(&l[i], &r[i], fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(data.finish(fragment.fragment().text()))
}

fn add_numeric_clone<L, R>(
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
	<L as Promote<R>>::Output: SafeAdd,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target.clone(), l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_val), Some(r_val)) => {
				let l_clone = l_val.clone();
				let r_clone = r_val.clone();
				match ctx.add(&l_clone, &r_clone, fragment)? {
					Some(value) => match ctx.fit_family(value, &target, fragment)? {
						Some(value) => data.push(value),
						None => data.push_none(),
					},
					None => data.push_none(),
				}
			}
			_ => data.push_none(),
		}
	}
	Ok(data.finish(fragment.fragment().text()))
}

fn can_promote_to_string(data: &ViewData) -> bool {
	matches!(
		data,
		ViewData::Bool(_)
			| ViewData::Float4(_)
			| ViewData::Float8(_)
			| ViewData::Int1(_)
			| ViewData::Int2(_)
			| ViewData::Int4(_)
			| ViewData::Int8(_)
			| ViewData::Int16(_)
			| ViewData::Uint1(_)
			| ViewData::Uint2(_)
			| ViewData::Uint4(_)
			| ViewData::Uint8(_)
			| ViewData::Uint16(_)
			| ViewData::Date(_)
			| ViewData::DateTime(_)
			| ViewData::Time(_)
			| ViewData::Duration(_)
			| ViewData::Uuid4(_)
			| ViewData::Uuid7(_)
			| ViewData::Blob { .. }
			| ViewData::Decimal { .. }
	)
}

fn concat_strings(
	l: &LargeStringArray,
	r: &LargeStringArray,
	target: ValueType,
	fragment: Fragment,
) -> Result<(FieldRef, ArrayRef)> {
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target, l.len());
	for i in 0..l.len() {
		match (get(l, i), get(r, i)) {
			(Some(l_str), Some(r_str)) => {
				let concatenated = format!("{}{}", l_str, r_str);
				data.push(concatenated);
			}
			_ => data.push_none(),
		}
	}
	Ok(data.finish(fragment.text()))
}

fn concat_string_with_other(
	string_data: &LargeStringArray,
	other_data: &ColumnView,
	string_is_left: bool,
	target: ValueType,
	fragment: Fragment,
) -> Result<(FieldRef, ArrayRef)> {
	reifydb_assertions! {
		assert_eq!(string_data.len(), other_data.len());
	}

	let mut data = ColumnBuilder::with_capacity(target, string_data.len());
	for i in 0..string_data.len() {
		match (get(string_data, i), other_data.is_defined(i)) {
			(Some(str_val), true) => {
				let other_str = other_data.as_string(i);
				let concatenated = if string_is_left {
					format!("{}{}", str_val, other_str)
				} else {
					format!("{}{}", other_str, str_val)
				};
				data.push(concatenated);
			}
			_ => data.push_none(),
		}
	}
	Ok(data.finish(fragment.text()))
}
