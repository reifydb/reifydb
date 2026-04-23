// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, push::Push};
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
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName> {
	binary_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let target = Type::promote(left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data(), &right.data();
			fixed: add_numeric, arb: add_numeric_clone (ctx, target, fragment);

			// Duration + Duration
			(ColumnBuffer::Duration(l), ColumnBuffer::Duration(r)) => {
				let mut container = TemporalContainer::with_capacity(l.len());
				for i in 0..l.len() {
					match (l.get(i), r.get(i)) {
						(Some(lv), Some(rv)) => container.push(*lv + *rv),
						_ => container.push_default(),
					}
				}
				Ok(ColumnWithName::new(fragment.fragment(), ColumnBuffer::Duration(container)))
			}

			// String concatenation
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

			// String + Other types (auto-promote to string)
			(
				ColumnBuffer::Utf8 {
					container: l,
					..
				},
				r,
			) if can_promote_to_string(r) => concat_string_with_other(l, r, true, target, fragment.fragment()),

			// Other types + String (auto-promote to string)
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
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeAdd,
	ColumnBuffer: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnBuffer::with_capacity(target, l.len());
	let l_data = l.data();
	let r_data = r.data();
	for i in 0..l.len() {
		if let Some(value) = ctx.add(&l_data[i], &r_data[i], fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(ColumnWithName {
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
) -> Result<ColumnWithName>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeAdd,
	ColumnBuffer: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnBuffer::with_capacity(target, l.len());
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
	Ok(ColumnWithName {
		name: fragment.fragment(),
		data,
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
			| ColumnBuffer::Int { .. }
			| ColumnBuffer::Uint { .. }
			| ColumnBuffer::Decimal { .. }
	)
}

fn concat_strings(l: &Utf8Container, r: &Utf8Container, target: Type, fragment: Fragment) -> Result<ColumnWithName> {
	debug_assert_eq!(l.len(), r.len());

	let mut data = ColumnBuffer::with_capacity(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_str), Some(r_str)) => {
				let concatenated = format!("{}{}", l_str, r_str);
				data.push(concatenated);
			}
			_ => data.push_none(),
		}
	}
	Ok(ColumnWithName {
		name: fragment,
		data,
	})
}

fn concat_string_with_other(
	string_data: &Utf8Container,
	other_data: &ColumnBuffer,
	string_is_left: bool,
	target: Type,
	fragment: Fragment,
) -> Result<ColumnWithName> {
	debug_assert_eq!(string_data.len(), other_data.len());

	let mut data = ColumnBuffer::with_capacity(target, string_data.len());
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
	Ok(ColumnWithName {
		name: fragment,
		data,
	})
}
