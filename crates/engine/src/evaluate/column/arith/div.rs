// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData, push::Push};
use reifydb_type::{
	error::diagnostic::operator::div_cannot_be_applied_to_incompatible_types,
	fragment::LazyFragment,
	return_error,
	value::{
		container::{number::NumberContainer, undefined::UndefinedContainer},
		is::IsNumber,
		number::{promote::Promote, safe::div::SafeDiv},
		r#type::{Type, get::GetType},
	},
};

use crate::evaluate::ColumnEvaluationContext;

pub(crate) fn div_columns(
	ctx: &ColumnEvaluationContext,
	left: &Column,
	right: &Column,
	fragment: impl LazyFragment + Copy,
) -> crate::Result<Column> {
	let target = Type::promote(left.get_type(), right.get_type());

	match (&left.data(), &right.data()) {
		// Float4
		(ColumnData::Float4(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float4(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int1(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint1(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),

		// Float8
		(ColumnData::Float8(l), ColumnData::Float4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Float8(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int1(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint1(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Float8(r)) => div_numeric(ctx, l, r, target, fragment),

		// Signed × Signed
		(ColumnData::Int1(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int1(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int1(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int1(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int1(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int2(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int4(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int8(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int16(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		// Signed × Unsigned
		(ColumnData::Int1(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int1(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int1(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int1(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int1(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int2(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int2(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int4(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int4(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int8(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int8(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Int16(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Int16(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		// Unsigned × Signed
		(ColumnData::Uint1(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint1(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint1(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint1(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint1(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint2(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint4(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint8(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint16(l), ColumnData::Int1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Int2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Int4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Int8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Int16(r)) => div_numeric(ctx, l, r, target, fragment),

		// Unsigned × Unsigned
		(ColumnData::Uint1(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint1(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint1(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint1(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint1(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint2(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint2(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint4(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint4(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint8(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint8(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		(ColumnData::Uint16(l), ColumnData::Uint1(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Uint2(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Uint4(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Uint8(r)) => div_numeric(ctx, l, r, target, fragment),
		(ColumnData::Uint16(l), ColumnData::Uint16(r)) => div_numeric(ctx, l, r, target, fragment),

		// Int with other types
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),

		// Uint with other types
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),

		// Decimal with other types
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => div_numeric_clone(ctx, l, r, target, fragment),

		// Standard types with Int, Uint, Decimal
		(
			ColumnData::Int1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Int16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),

		(
			ColumnData::Uint1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Uint16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),

		(
			ColumnData::Float4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Float4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Float4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Float8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Float8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),
		(
			ColumnData::Float8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => div_numeric_clone(ctx, l, r, target, fragment),

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

		_ => return_error!(div_cannot_be_applied_to_incompatible_types(
			fragment.fragment(),
			left.get_type(),
			right.get_type(),
		)),
	}
}

fn div_numeric<'a, L, R>(
	ctx: &ColumnEvaluationContext,
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

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let mut data = ctx.pooled(target, l.len());
		let l_data = l.data();
		let r_data = r.data();

		for i in 0..l.len() {
			if let Some(value) = ctx.div(&l_data[i], &r_data[i], fragment)? {
				data.push(value);
			} else {
				data.push_undefined()
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	} else {
		// Slow path: some values may be undefined
		let mut data = ctx.pooled(target, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					if let Some(value) = ctx.div(l, r, fragment)? {
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
}

fn div_numeric_clone<'a, L, R>(
	ctx: &ColumnEvaluationContext,
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

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let mut data = ctx.pooled(target, l.len());
		let l_data = l.data();
		let r_data = r.data();

		for i in 0..l.len() {
			let l_clone = l_data[i].clone();
			let r_clone = r_data[i].clone();
			if let Some(value) = ctx.div(&l_clone, &r_clone, fragment)? {
				data.push(value);
			} else {
				data.push_undefined()
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	} else {
		// Slow path: some values may be undefined
		let mut data = ctx.pooled(target, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l_val), Some(r_val)) => {
					let l_clone = l_val.clone();
					let r_clone = r_val.clone();
					if let Some(value) = ctx.div(&l_clone, &r_clone, fragment)? {
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
}
