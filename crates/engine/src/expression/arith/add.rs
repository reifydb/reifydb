// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData, push::Push};
use reifydb_type::{
	error::{BinaryOp, TypeError},
	fragment::{Fragment, LazyFragment},
	value::{
		container::{number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container},
		is::IsNumber,
		number::{promote::Promote, safe::add::SafeAdd},
		r#type::{Type, get::GetType},
	},
};

use crate::{
	Result,
	expression::{context::EvalContext, option::binary_op_unwrap_option},
};

pub(crate) fn add_columns(
	ctx: &EvalContext,
	left: &Column,
	right: &Column,
	fragment: impl LazyFragment + Copy,
) -> Result<Column> {
	binary_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let target = Type::promote(left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data(), &right.data();
			fixed: add_numeric, arb: add_numeric_clone (ctx, target, fragment);

			// Duration + Duration
			(ColumnData::Duration(l), ColumnData::Duration(r)) => {
				let mut container = TemporalContainer::with_capacity(l.len());
				for i in 0..l.len() {
					match (l.get(i), r.get(i)) {
						(Some(lv), Some(rv)) => container.push(*lv + *rv),
						_ => container.push_default(),
					}
				}
				Ok(Column {
					name: fragment.fragment(),
					data: ColumnData::Duration(container),
				})
			}

			// String concatenation
			(
				ColumnData::Utf8 {
					container: l,
					..
				},
				ColumnData::Utf8 {
					container: r,
					..
				},
			) => concat_strings(l, r, target, fragment.fragment()),

			// String + Other types (auto-promote to string)
			(
				ColumnData::Utf8 {
					container: l,
					..
				},
				r,
			) if can_promote_to_string(r) => concat_string_with_other(l, r, true, target, fragment.fragment()),

			// Other types + String (auto-promote to string)
			(
				l,
				ColumnData::Utf8 {
					container: r,
					..
				},
			) if can_promote_to_string(l) => concat_string_with_other(r, l, false, target, fragment.fragment()),

			_ => return Err(TypeError::BinaryOperatorNotApplicable {
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
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment + Copy,
) -> Result<Column>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeAdd,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnData::with_capacity(target, l.len());
	let l_data = l.data();
	let r_data = r.data();
	for i in 0..l.len() {
		if let Some(value) = ctx.add(&l_data[i], &r_data[i], fragment)? {
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

fn add_numeric_clone<L, R>(
	ctx: &EvalContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment + Copy,
) -> Result<Column>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeAdd,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnData::with_capacity(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_val), Some(r_val)) => {
				let l_clone = l_val.clone();
				let r_clone = r_val.clone();
				if let Some(value) = ctx.add(&l_clone, &r_clone, fragment)? {
					data.push(value);
				} else {
					data.push_none()
				}
			}
			_ => data.push_none(),
		}
	}
	Ok(Column {
		name: fragment.fragment(),
		data,
	})
}

fn can_promote_to_string(data: &ColumnData) -> bool {
	matches!(
		data,
		ColumnData::Bool(_)
			| ColumnData::Float4(_)
			| ColumnData::Float8(_)
			| ColumnData::Int1(_) | ColumnData::Int2(_)
			| ColumnData::Int4(_) | ColumnData::Int8(_)
			| ColumnData::Int16(_)
			| ColumnData::Uint1(_)
			| ColumnData::Uint2(_)
			| ColumnData::Uint4(_)
			| ColumnData::Uint8(_)
			| ColumnData::Uint16(_)
			| ColumnData::Date(_) | ColumnData::DateTime(_)
			| ColumnData::Time(_) | ColumnData::Duration(_)
			| ColumnData::Uuid4(_)
			| ColumnData::Uuid7(_)
			| ColumnData::Blob { .. }
			| ColumnData::Int { .. }
			| ColumnData::Uint { .. }
			| ColumnData::Decimal { .. }
	)
}

fn concat_strings(l: &Utf8Container, r: &Utf8Container, target: Type, fragment: Fragment) -> Result<Column> {
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnData::with_capacity(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_str), Some(r_str)) => {
				let concatenated = format!("{}{}", l_str, r_str);
				data.push(concatenated);
			}
			_ => data.push_none(),
		}
	}
	Ok(Column {
		name: fragment,
		data,
	})
}

fn concat_string_with_other(
	string_data: &Utf8Container,
	other_data: &ColumnData,
	string_is_left: bool,
	target: Type,
	fragment: Fragment,
) -> Result<Column> {
	debug_assert_eq!(string_data.len(), other_data.len());

	let mut data = ColumnData::with_capacity(target, string_data.len());
	for i in 0..string_data.len() {
		match (string_data.get(i), other_data.is_defined(i)) {
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
	Ok(Column {
		name: fragment,
		data,
	})
}
