// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::Debug;

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{
	fragment::Fragment,
	value::{
		container::{
			bool::BoolContainer, number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container,
			uuid::UuidContainer,
		},
		is::{IsNumber, IsTemporal, IsUuid},
		number::promote::Promote,
		r#type::get::GetType,
	},
};

use crate::expression::types::{EvalError, EvalResult};

pub(crate) fn eval_eq(left: &Column, right: &Column) -> EvalResult<Column> {
	match (left.data(), right.data()) {
		// Bool comparisons
		(ColumnData::Bool(l), ColumnData::Bool(r)) => compare_bool_eq(l, r),

		// Float4 with all numeric types
		(ColumnData::Float4(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float4(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Float4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Float4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Float4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Float8 with all numeric types
		(ColumnData::Float8(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Float8(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Float8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Float8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Float8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Int1 with all numeric types
		(ColumnData::Int1(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int1(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Int1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Int2 with all numeric types
		(ColumnData::Int2(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int2(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Int2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Int4 with all numeric types
		(ColumnData::Int4(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int4(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Int4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Int8 with all numeric types
		(ColumnData::Int8(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int8(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Int8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Int16 with all numeric types
		(ColumnData::Int16(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Int16(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Int16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Uint1 with all numeric types
		(ColumnData::Uint1(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint1(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint1(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint1(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint1(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Uint2 with all numeric types
		(ColumnData::Uint2(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint2(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint2(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint2(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint2(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Uint4 with all numeric types
		(ColumnData::Uint4(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint4(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint4(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint4(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint4(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Uint8 with all numeric types
		(ColumnData::Uint8(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint8(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint8(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint8(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint8(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Uint16 with all numeric types
		(ColumnData::Uint16(l), ColumnData::Float4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Float8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Int1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Int2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Int4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Int8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Int16(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint1(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint2(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint4(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint8(r)) => compare_numeric_eq(l, r),
		(ColumnData::Uint16(l), ColumnData::Uint16(r)) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint16(l),
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint16(l),
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint16(l),
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Int (arbitrary precision) with all numeric types
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Int {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Uint (arbitrary precision) with all numeric types
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Uint {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Decimal (arbitrary precision) with all numeric types
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Float8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int1(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int2(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int16(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint1(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint2(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint4(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint8(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint16(r),
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Int {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Uint {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),
		(
			ColumnData::Decimal {
				container: l,
				..
			},
			ColumnData::Decimal {
				container: r,
				..
			},
		) => compare_numeric_eq(l, r),

		// Temporal types (same-type only)
		(ColumnData::Date(l), ColumnData::Date(r)) => compare_temporal_eq(l, r),
		(ColumnData::DateTime(l), ColumnData::DateTime(r)) => compare_temporal_eq(l, r),
		(ColumnData::Time(l), ColumnData::Time(r)) => compare_temporal_eq(l, r),
		(ColumnData::Duration(l), ColumnData::Duration(r)) => compare_temporal_eq(l, r),

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
		) => compare_text_eq(l, r),

		// UUID comparisons (same-type only)
		(ColumnData::Uuid4(l), ColumnData::Uuid4(r)) => compare_uuid_eq(l, r),
		(ColumnData::Uuid7(l), ColumnData::Uuid7(r)) => compare_uuid_eq(l, r),

		// Undefined propagation
		(ColumnData::Undefined(container), _) | (_, ColumnData::Undefined(container)) => {
			let mut data = Vec::with_capacity(container.len());
			let mut bitvec = Vec::with_capacity(container.len());
			for _ in 0..container.len() {
				data.push(false);
				bitvec.push(false);
			}
			Ok(Column::new(Fragment::internal("_eq"), ColumnData::bool_with_bitvec(data, bitvec)))
		}

		// Type mismatch error for incompatible types
		_ => Err(EvalError::TypeMismatch {
			expected: format!("{:?}", left.data().get_type()),
			found: format!("{:?}", right.data().get_type()),
			context: "EQ operands".to_string(),
		}),
	}
}

/// Helper function for numeric equality with type promotion
fn compare_numeric_eq<L, R>(left: &NumberContainer<L>, right: &NumberContainer<R>) -> EvalResult<Column>
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
				let is_eq = match lp.partial_cmp(&rp) {
					Some(std::cmp::Ordering::Equal) => true,
					_ => false,
				};
				data.push(is_eq);
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
						let is_eq = match lp.partial_cmp(&rp) {
							Some(std::cmp::Ordering::Equal) => true,
							_ => false,
						};
						data.push(is_eq);
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

	Ok(Column::new(Fragment::internal("_eq"), ColumnData::bool_with_bitvec(data, bitvec)))
}

/// Helper function for boolean equality
fn compare_bool_eq(left: &BoolContainer, right: &BoolContainer) -> EvalResult<Column> {
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);

	// Fast path: both fully defined
	if left.is_fully_defined() && right.is_fully_defined() {
		for (lv, rv) in left.data().iter().zip(right.data().iter()) {
			data.push(lv == rv);
			bitvec.push(true);
		}
	} else {
		// Slow path: check undefineds
		for i in 0..row_count {
			match (left.get(i), right.get(i)) {
				(Some(lv), Some(rv)) => {
					data.push(lv == rv);
					bitvec.push(true);
				}
				_ => {
					data.push(false);
					bitvec.push(false);
				}
			}
		}
	}

	Ok(Column::new(Fragment::internal("_eq"), ColumnData::bool_with_bitvec(data, bitvec)))
}

/// Helper function for temporal equality
fn compare_temporal_eq<T>(left: &TemporalContainer<T>, right: &TemporalContainer<T>) -> EvalResult<Column>
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
			data.push(left_data[i] == right_data[i]);
			bitvec.push(true);
		}
	} else {
		// Slow path: check undefineds
		for i in 0..row_count {
			match (left.get(i), right.get(i)) {
				(Some(lv), Some(rv)) => {
					data.push(lv == rv);
					bitvec.push(true);
				}
				_ => {
					data.push(false);
					bitvec.push(false);
				}
			}
		}
	}

	Ok(Column::new(Fragment::internal("_eq"), ColumnData::bool_with_bitvec(data, bitvec)))
}

/// Helper function for text equality
fn compare_text_eq(left: &Utf8Container, right: &Utf8Container) -> EvalResult<Column> {
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);

	// Fast path: both fully defined
	if left.is_fully_defined() && right.is_fully_defined() {
		let left_data = left.data();
		let right_data = right.data();

		for i in 0..row_count {
			data.push(left_data[i] == right_data[i]);
			bitvec.push(true);
		}
	} else {
		// Slow path: check undefineds
		for i in 0..row_count {
			match (left.get(i), right.get(i)) {
				(Some(lv), Some(rv)) => {
					data.push(lv == rv);
					bitvec.push(true);
				}
				_ => {
					data.push(false);
					bitvec.push(false);
				}
			}
		}
	}

	Ok(Column::new(Fragment::internal("_eq"), ColumnData::bool_with_bitvec(data, bitvec)))
}

fn compare_uuid_eq<T>(left: &UuidContainer<T>, right: &UuidContainer<T>) -> EvalResult<Column>
where
	T: Copy + PartialEq + Debug + Default,
	T: IsUuid,
{
	debug_assert_eq!(left.len(), right.len());

	let row_count = left.len();
	let mut data = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);

	for i in 0..row_count {
		match (left.get(i), right.get(i)) {
			(Some(lv), Some(rv)) => {
				data.push(lv == rv);
				bitvec.push(true);
			}
			_ => {
				data.push(false);
				bitvec.push(false);
			}
		}
	}

	Ok(Column::new(Fragment::internal("_eq"), ColumnData::bool_with_bitvec(data, bitvec)))
}
