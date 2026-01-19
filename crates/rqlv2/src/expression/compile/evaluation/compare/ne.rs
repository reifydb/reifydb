// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{
	fragment::Fragment,
	value::{
		container::{
			bool::BoolContainer, number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container,
			uuid::UuidContainer,
		},
		is::{IsNumber, IsTemporal},
		number::promote::Promote,
		r#type::get::GetType,
	},
};

use crate::expression::types::{EvalError, EvalResult};

pub(crate) fn eval_ne(left: &Column, right: &Column) -> EvalResult<Column> {
	match (left.data(), right.data()) {
		// Bool comparisons
		(ColumnData::Bool(l), ColumnData::Bool(r)) => compare_bool_ne(l, r),

		// Float4 with all numeric types
		(ColumnData::Float4(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float4(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Float4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Float4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Float4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Float8 with all numeric types
		(ColumnData::Float8(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Float8(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Float8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Float8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Float8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Int1 with all numeric types
		(ColumnData::Int1(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int1(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Int1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Int2 with all numeric types
		(ColumnData::Int2(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int2(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Int2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Int4 with all numeric types
		(ColumnData::Int4(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int4(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Int4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Int8 with all numeric types
		(ColumnData::Int8(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int8(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Int8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Int16 with all numeric types
		(ColumnData::Int16(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Int16(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Int16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Uint1 with all numeric types
		(ColumnData::Uint1(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Uint2 with all numeric types
		(ColumnData::Uint2(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Uint4 with all numeric types
		(ColumnData::Uint4(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Uint8 with all numeric types
		(ColumnData::Uint8(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Uint16 with all numeric types
		(ColumnData::Uint16(l), ColumnData::Float4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Float8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Int1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Int2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Int4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Int8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Int16(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint1(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint2(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint4(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint8(r)) => compare_numeric_ne(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint16(r)) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Int (arbitrary precision) with all numeric types
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Uint (arbitrary precision) with all numeric types
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Decimal (arbitrary precision) with all numeric types
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_ne(l, r),

		// Temporal types (same-type only)
		(ColumnData::Date(l), ColumnData::Date(r)) => compare_temporal_ne(l, r),
		(ColumnData::DateTime(l), ColumnData::DateTime(r)) => compare_temporal_ne(l, r),
		(ColumnData::Time(l), ColumnData::Time(r)) => compare_temporal_ne(l, r),
		(ColumnData::Duration(l), ColumnData::Duration(r)) => compare_temporal_ne(l, r),

		// Text comparison
		(
			ColumnData::Utf8 {
				container: l,
				..
			},
			ColumnData::Utf8 {
				container: r,
				..
			},
		) => compare_text_ne(l, r),

		// UUID comparisons (same-type only)
		(ColumnData::Uuid4(l), ColumnData::Uuid4(r)) => compare_uuid_ne(l, r),
		(ColumnData::Uuid7(l), ColumnData::Uuid7(r)) => compare_uuid_ne(l, r),

		// Undefined propagation
		(ColumnData::Undefined(container), _) | (_, ColumnData::Undefined(container)) => {
			let mut data = Vec::with_capacity(container.len());
			let mut bitvec = Vec::with_capacity(container.len());
			for _ in 0..container.len() {
				data.push(false);
				bitvec.push(false);
			}
			Ok(Column::new(
				Fragment::internal("_ne"),
				ColumnData::bool_with_bitvec(data, bitvec),
			))
		}

		// Type mismatch error for incompatible types
		_ => Err(EvalError::TypeMismatch {
			expected: format!("{:?}", left.data().get_type()),
			found: format!("{:?}", right.data().get_type()),
			context: "NE operands".to_string(),
		}),
	}
}

/// Helper function for numeric inequality with type promotion
fn compare_numeric_ne<L, R>(left: &NumberContainer<L>, right: &NumberContainer<R>) -> EvalResult<Column>
where
	L: GetType + Promote<R> + IsNumber + PartialEq,
	R: GetType + IsNumber + PartialEq,
	<L as Promote<R>>::Output: IsNumber + PartialEq,
{
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);

	// Fast path: both fully defined
	if left.is_fully_defined() && right.is_fully_defined() {
		let left_data = left.data();
		let right_data = right.data();

		for i in 0..row_count {
			if let Some((lp, rp)) = left_data[i].checked_promote(&right_data[i]) {
				// Use partial_cmp to handle NaN cases properly
				let is_ne = match lp.partial_cmp(&rp) {
					Some(std::cmp::Ordering::Equal) => false,
					_ => true,
				};
				data.push(is_ne);
				bitvec.push(true);
			} else {
				// Promotion failed → undefined
				data.push(false);
				bitvec.push(false);
			}
		}
	} else {
		// Slow path: check undefineds
		for i in 0..row_count {
			match (left.get(i), right.get(i)) {
				(Some(lv), Some(rv)) => {
					if let Some((lp, rp)) = lv.checked_promote(rv) {
						let is_ne = match lp.partial_cmp(&rp) {
							Some(std::cmp::Ordering::Equal) => false,
							_ => true,
						};
						data.push(is_ne);
						bitvec.push(true);
					} else {
						data.push(false);
						bitvec.push(false);
					}
				}
				_ => {
					// Null input → undefined
					data.push(false);
					bitvec.push(false);
				}
			}
		}
	}

	Ok(Column::new(
		Fragment::internal("_ne"),
		ColumnData::bool_with_bitvec(data, bitvec),
	))
}

/// Helper function for boolean inequality
fn compare_bool_ne(left: &BoolContainer, right: &BoolContainer) -> EvalResult<Column> {
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);

	// Fast path: both fully defined
	if left.is_fully_defined() && right.is_fully_defined() {
		for (lv, rv) in left.data().iter().zip(right.data().iter()) {
			data.push(lv != rv);
			bitvec.push(true);
		}
	} else {
		// Slow path: check undefineds
		for i in 0..row_count {
			match (left.get(i), right.get(i)) {
				(Some(lv), Some(rv)) => {
					data.push(lv != rv);
					bitvec.push(true);
				}
				_ => {
					data.push(false);
					bitvec.push(false);
				}
			}
		}
	}

	Ok(Column::new(
		Fragment::internal("_ne"),
		ColumnData::bool_with_bitvec(data, bitvec),
	))
}

/// Helper function for temporal inequality
fn compare_temporal_ne<T>(left: &TemporalContainer<T>, right: &TemporalContainer<T>) -> EvalResult<Column>
where
	T: IsTemporal + Copy + PartialEq,
{
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);

	// Fast path: both fully defined
	if left.is_fully_defined() && right.is_fully_defined() {
		let left_data = left.data();
		let right_data = right.data();

		for i in 0..row_count {
			data.push(left_data[i] != right_data[i]);
			bitvec.push(true);
		}
	} else {
		// Slow path: check undefineds
		for i in 0..row_count {
			match (left.get(i), right.get(i)) {
				(Some(lv), Some(rv)) => {
					data.push(lv != rv);
					bitvec.push(true);
				}
				_ => {
					data.push(false);
					bitvec.push(false);
				}
			}
		}
	}

	Ok(Column::new(
		Fragment::internal("_ne"),
		ColumnData::bool_with_bitvec(data, bitvec),
	))
}

/// Helper function for text inequality
fn compare_text_ne(left: &Utf8Container, right: &Utf8Container) -> EvalResult<Column> {
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);

	// Fast path: both fully defined
	if left.is_fully_defined() && right.is_fully_defined() {
		let left_data = left.data();
		let right_data = right.data();

		for i in 0..row_count {
			data.push(left_data[i] != right_data[i]);
			bitvec.push(true);
		}
	} else {
		// Slow path: check undefineds
		for i in 0..row_count {
			match (left.get(i), right.get(i)) {
				(Some(lv), Some(rv)) => {
					data.push(lv != rv);
					bitvec.push(true);
				}
				_ => {
					data.push(false);
					bitvec.push(false);
				}
			}
		}
	}

	Ok(Column::new(
		Fragment::internal("_ne"),
		ColumnData::bool_with_bitvec(data, bitvec),
	))
}

/// Helper function for UUID inequality
fn compare_uuid_ne<T>(left: &UuidContainer<T>, right: &UuidContainer<T>) -> EvalResult<Column>
where
	T: Copy + PartialEq + std::fmt::Debug + Default,
	T: reifydb_type::value::is::IsUuid,
{
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);

	for i in 0..row_count {
		match (left.get(i), right.get(i)) {
			(Some(lv), Some(rv)) => {
				data.push(lv != rv);
				bitvec.push(true);
			}
			_ => {
				data.push(false);
				bitvec.push(false);
			}
		}
	}

	Ok(Column::new(
		Fragment::internal("_ne"),
		ColumnData::bool_with_bitvec(data, bitvec),
	))

}
