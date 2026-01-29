// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData, push::Push};
use reifydb_type::{
	fragment::Fragment,
	value::{
		container::number::NumberContainer,
		is::IsNumber,
		number::{promote::Promote, safe::sub::SafeSub},
		r#type::{Type, get::GetType},
	},
};

use crate::expression::types::{EvalError, EvalResult};

pub(crate) fn eval_sub(left: &Column, right: &Column) -> EvalResult<Column> {
	let target = Type::promote(left.get_type(), right.get_type());

	match (left.data(), right.data()) {
		// Float4 combinations
		(ColumnData::Float4(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Float4(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int1(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint1(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),

		// Float8 combinations
		(ColumnData::Float8(l), ColumnData::Float4(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Float8(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int1(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint1(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Float8(r)) => sub_numeric(l, r, target),

		// Signed × Signed
		(ColumnData::Int1(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int1(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int1(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int1(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int1(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int2(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int4(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int8(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int16(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		// Signed × Unsigned
		(ColumnData::Int1(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int1(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int1(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int1(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int1(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int2(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int2(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int4(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int4(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int8(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int8(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Int16(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Int16(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		// Unsigned × Signed
		(ColumnData::Uint1(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint1(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint1(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint1(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint1(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint2(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint4(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint8(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint16(l), ColumnData::Int1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Int2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Int4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Int8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Int16(r)) => sub_numeric(l, r, target),

		// Unsigned × Unsigned
		(ColumnData::Uint1(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint1(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint1(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint1(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint1(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint2(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint2(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint4(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint4(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint8(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint8(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		(ColumnData::Uint16(l), ColumnData::Uint1(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Uint2(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Uint4(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Uint8(r)) => sub_numeric(l, r, target),
		(ColumnData::Uint16(l), ColumnData::Uint16(r)) => sub_numeric(l, r, target),

		// Int (arbitrary precision) operations
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Int with transaction numeric types
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => sub_numeric_clone(l, r, target),

		// Uint (arbitrary precision) operations
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Uint with transaction numeric types
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => sub_numeric_clone(l, r, target),

		// Decimal (arbitrary precision) operations
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Decimal with transaction numeric types
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => sub_numeric_clone(l, r, target),

		// Reverse operations for transaction types with Int, Uint, Decimal
		// Float4 with Int, Uint, Decimal
		(
			ColumnData::Float4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Float4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Float4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Float8 with Int, Uint, Decimal
		(
			ColumnData::Float8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Float8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Float8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Int1 with Int, Uint, Decimal
		(
			ColumnData::Int1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Int2 with Int, Uint, Decimal
		(
			ColumnData::Int2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Int4 with Int, Uint, Decimal
		(
			ColumnData::Int4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Int8 with Int, Uint, Decimal
		(
			ColumnData::Int8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Int16 with Int, Uint, Decimal
		(
			ColumnData::Int16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Int16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Uint1 with Int, Uint, Decimal
		(
			ColumnData::Uint1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Uint2 with Int, Uint, Decimal
		(
			ColumnData::Uint2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Uint4 with Int, Uint, Decimal
		(
			ColumnData::Uint4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Uint8 with Int, Uint, Decimal
		(
			ColumnData::Uint8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		// Uint16 with Int, Uint, Decimal
		(
			ColumnData::Uint16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),
		(
			ColumnData::Uint16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => sub_numeric_clone(l, r, target),

		_ => Err(EvalError::TypeMismatch {
			expected: format!("{:?}", left.get_type()),
			found: format!("{:?}", right.get_type()),
			context: "ADD operands".to_string(),
		}),
	}
}

/// Generic helper function for Copy types (i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, f32, f64)
fn sub_numeric<L, R>(left: &NumberContainer<L>, right: &NumberContainer<R>, target: Type) -> EvalResult<Column>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber + SafeSub,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = ColumnData::with_capacity(target, row_count);

	// Fast path: both fully defined
	if left.is_fully_defined() && right.is_fully_defined() {
		let left_data = left.data();
		let right_data = right.data();

		for i in 0..row_count {
			// checked_promote returns Option<(promoted_l, promoted_r)>
			if let Some((lp, rp)) = left_data[i].checked_promote(&right_data[i]) {
				// checked_sub returns Option - None on overflow
				if let Some(diff) = lp.checked_sub(&rp) {
					data.push(diff);
				} else {
					// Overflow → undefined
					data.push_undefined();
				}
			} else {
				// Promotion failed → undefined (shouldn't happen for valid numeric types)
				data.push_undefined();
			}
		}
	} else {
		// Slow path: check undefineds for each element
		for i in 0..row_count {
			match (left.get(i), right.get(i)) {
				(Some(lv), Some(rv)) => {
					if let Some((lp, rp)) = lv.checked_promote(rv) {
						if let Some(diff) = lp.checked_sub(&rp) {
							data.push(diff);
						} else {
							data.push_undefined();
						}
					} else {
						data.push_undefined();
					}
				}
				_ => {
					// Null input → undefined
					data.push_undefined();
				}
			}
		}
	}

	Ok(Column::new(Fragment::internal("_sub"), data))
}

/// Generic helper function for Clone types (Int, Uint, Decimal)
fn sub_numeric_clone<L, R>(left: &NumberContainer<L>, right: &NumberContainer<R>, target: Type) -> EvalResult<Column>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber + SafeSub,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = ColumnData::with_capacity(target, row_count);

	for i in 0..row_count {
		match (left.get(i), right.get(i)) {
			(Some(lv), Some(rv)) => {
				let l_clone = lv.clone();
				let r_clone = rv.clone();
				if let Some((lp, rp)) = l_clone.checked_promote(&r_clone) {
					if let Some(diff) = lp.checked_sub(&rp) {
						data.push(diff);
					} else {
						data.push_undefined();
					}
				} else {
					data.push_undefined();
				}
			}
			_ => data.push_undefined(),
		}
	}

	Ok(Column::new(Fragment::internal("_sub"), data))
}
