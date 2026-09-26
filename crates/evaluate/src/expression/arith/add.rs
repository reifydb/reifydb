// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, LargeStringArray};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder, push::Push};
use reifydb_value::{
	error::{BinaryOp, TypeError},
	fragment::{Fragment, LazyFragment},
	reifydb_assertions,
	value::{
		container::{
			decimal_array::{decimals, u128s},
			temporal_array::{duration_array, durations},
			varlen_array::get,
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
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName> {
	arith_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let target = arith_target(ArithOp::Add, left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data(), &right.data();
			fixed: add_numeric, arb: add_numeric_clone (ctx, target, fragment);


			(ColumnBuffer::Duration(l), ColumnBuffer::Duration(r)) => {
				let (l, r) = (durations(l), durations(r));
				let values = (0..l.len())
					.map(|i| match (l.get(i), r.get(i)) {
						(Some(lv), Some(rv)) => lv.try_add(*rv).map_err(|e| (*e).into()),
						_ => Err(length_mismatch(l.len(), r.len(), &fragment.fragment())),
					})
					.collect::<Result<Vec<_>>>()?;
				Ok(ColumnWithName::new(fragment.fragment(), ColumnBuffer::Duration(duration_array(values))))
			}


			(
				ColumnBuffer::Utf8 {
					container: l,
					..
				},
				ColumnBuffer::Utf8 {
					container: r,
					..
				},
			) => concat_strings(l, r, target, fragment.fragment()),


			(
				ColumnBuffer::Utf8 {
					container: l,
					..
				},
				r,
			) if can_promote_to_string(r) => concat_string_with_other(l, r, true, target, fragment.fragment()),


			(
				l,
				ColumnBuffer::Utf8 {
					container: r,
					..
				},
			) if can_promote_to_string(l) => concat_string_with_other(r, l, false, target, fragment.fragment()),

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
) -> Result<ColumnWithName>
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
	Ok(ColumnWithName {
		name: fragment.fragment(),
		data: data.finish(),
	})
}

fn add_numeric_clone<L, R>(
	ctx: &EvalContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName>
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
	Ok(ColumnWithName {
		name: fragment.fragment(),
		data: data.finish(),
	})
}

fn can_promote_to_string(data: &ColumnBuffer) -> bool {
	matches!(
		data,
		ColumnBuffer::Bool(_)
			| ColumnBuffer::Float4(_)
			| ColumnBuffer::Float8(_)
			| ColumnBuffer::Int1(_)
			| ColumnBuffer::Int2(_)
			| ColumnBuffer::Int4(_)
			| ColumnBuffer::Int8(_)
			| ColumnBuffer::Int16(_)
			| ColumnBuffer::Uint1(_)
			| ColumnBuffer::Uint2(_)
			| ColumnBuffer::Uint4(_)
			| ColumnBuffer::Uint8(_)
			| ColumnBuffer::Uint16(_)
			| ColumnBuffer::Date(_)
			| ColumnBuffer::DateTime(_)
			| ColumnBuffer::Time(_)
			| ColumnBuffer::Duration(_)
			| ColumnBuffer::Uuid4(_)
			| ColumnBuffer::Uuid7(_)
			| ColumnBuffer::Blob { .. }
			| ColumnBuffer::Decimal { .. }
	)
}

fn concat_strings(
	l: &LargeStringArray,
	r: &LargeStringArray,
	target: ValueType,
	fragment: Fragment,
) -> Result<ColumnWithName> {
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
	Ok(ColumnWithName {
		name: fragment,
		data: data.finish(),
	})
}

fn concat_string_with_other(
	string_data: &LargeStringArray,
	other_data: &ColumnBuffer,
	string_is_left: bool,
	target: ValueType,
	fragment: Fragment,
) -> Result<ColumnWithName> {
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
	Ok(ColumnWithName {
		name: fragment,
		data: data.finish(),
	})
}
