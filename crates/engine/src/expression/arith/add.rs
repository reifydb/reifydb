// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData, push::Push};
use reifydb_type::{
	error::diagnostic::operator::add_cannot_be_applied_to_incompatible_types,
	fragment::{Fragment, LazyFragment},
	return_error,
	value::{
		container::{
			number::NumberContainer, temporal::TemporalContainer, undefined::UndefinedContainer,
			utf8::Utf8Container,
		},
		is::IsNumber,
		number::{promote::Promote, safe::add::SafeAdd},
		r#type::{Type, get::GetType},
	},
};

use crate::expression::context::EvalContext;

pub(crate) fn add_columns(
	ctx: &EvalContext,
	left: &Column,
	right: &Column,
	fragment: impl LazyFragment + Copy,
) -> crate::Result<Column> {
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
					_ => container.push_undefined(),
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
		) => concat_strings(ctx, l, r, target, fragment.fragment()),

		// String + Other types (auto-promote to string)
		(
			ColumnData::Utf8 {
				container: l,
				..
			},
			r,
		) if can_promote_to_string(r) => concat_string_with_other(ctx, l, r, true, target, fragment.fragment()),

		// Other types + String (auto-promote to string)
		(
			l,
			ColumnData::Utf8 {
				container: r,
				..
			},
		) if can_promote_to_string(l) => concat_string_with_other(ctx, r, l, false, target, fragment.fragment()),

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

		_ => return_error!(add_cannot_be_applied_to_incompatible_types(
			fragment.fragment(),
			left.get_type(),
			right.get_type(),
		)),
	)
}

fn add_numeric<L, R>(
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
	<L as Promote<R>>::Output: SafeAdd,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	// Fast path: when both inputs are fully defined
	// We still need to handle potential overflow (which produces undefined
	// with Undefined policy)
	if l.is_fully_defined() && r.is_fully_defined() {
		let mut data = ctx.pooled(target, l.len());
		let l_data = l.data();
		let r_data = r.data();

		// Even with fully-defined inputs, operations can produce
		// undefined values due to overflow (with Undefined policy) or
		// other errors
		for i in 0..l.len() {
			// Safe to index directly since we know all values are
			// defined
			if let Some(value) = ctx.add(&l_data[i], &r_data[i], fragment)? {
				data.push(value);
			} else {
				// Overflow with Undefined policy produces
				// undefined
				data.push_undefined()
			}
		}

		return Ok(Column {
			name: fragment.fragment(),
			data,
		});
	}

	// Slow path: some input values may be undefined
	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				if let Some(value) = ctx.add(l, r, fragment)? {
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

fn add_numeric_clone<L, R>(
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
	<L as Promote<R>>::Output: SafeAdd,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_val), Some(r_val)) => {
				let l_clone = l_val.clone();
				let r_clone = r_val.clone();
				if let Some(value) = ctx.add(&l_clone, &r_clone, fragment)? {
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

fn concat_strings(
	ctx: &EvalContext,
	l: &Utf8Container,
	r: &Utf8Container,
	target: Type,
	fragment: Fragment,
) -> crate::Result<Column> {
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_str), Some(r_str)) => {
				let concatenated = format!("{}{}", l_str, r_str);
				data.push(concatenated);
			}
			_ => data.push_undefined(),
		}
	}
	Ok(Column {
		name: fragment,
		data,
	})
}

fn concat_string_with_other(
	ctx: &EvalContext,
	string_data: &Utf8Container,
	other_data: &ColumnData,
	string_is_left: bool,
	target: Type,
	fragment: Fragment,
) -> crate::Result<Column> {
	debug_assert_eq!(string_data.len(), other_data.len());

	let mut data = ctx.pooled(target, string_data.len());
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
			_ => data.push_undefined(),
		}
	}
	Ok(Column {
		name: fragment,
		data,
	})
}
