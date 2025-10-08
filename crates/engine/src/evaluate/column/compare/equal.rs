// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	return_error,
	value::{
		column::{Column, ColumnData},
		container::{BoolContainer, NumberContainer, TemporalContainer, Utf8Container},
	},
};
use reifydb_rql::expression::EqExpression;
use reifydb_type::{
	Decimal, Fragment, Int, IsNumber, IsTemporal, Promote, Type::Boolean, Uint,
	diagnostic::operator::equal_cannot_be_applied_to_incompatible_types, temporal, value::number,
};

use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

impl StandardColumnEvaluator {
	pub(crate) fn equal<'a>(
		&self,
		ctx: &ColumnEvaluationContext<'a>,
		eq: &EqExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let left = self.evaluate(ctx, &eq.left)?;
		let right = self.evaluate(ctx, &eq.right)?;

		match (&left.data(), &right.data()) {
			(ColumnData::Bool(l), ColumnData::Bool(r)) => {
				Ok(compare_bool(ctx, l, r, eq.full_fragment_owned()))
			}
			// Float4 with Int, Uint, Decimal
			(
				ColumnData::Float4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<f32, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Float4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<f32, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Float4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<f32, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Float4
			(ColumnData::Float4(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<f32, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<f32, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<f32, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<f32, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<f32, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<f32, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<f32, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<f32, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<f32, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<f32, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<f32, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<f32, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<f64, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<f64, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<f64, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<f64, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<f64, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<f64, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<f64, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<f64, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<f64, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<f64, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<f64, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<f64, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Float8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<f64, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Float8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<f64, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Float8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<f64, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Int1
			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i8, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i8, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i8, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i8, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i8, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i8, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i8, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i8, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i8, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i8, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i8, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i8, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Int1(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<i8, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int1(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<i8, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i8, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Int2
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i16, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i16, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i16, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i16, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i16, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i16, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i16, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i16, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i16, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i16, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i16, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i16, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Int2(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<i16, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int2(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<i16, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i16, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Int4
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i32, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i32, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i32, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i32, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i32, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i32, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i32, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i32, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i32, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i32, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i32, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i32, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Int4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<i32, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<i32, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i32, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Int8
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i64, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i64, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i64, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i64, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i64, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i64, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i64, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i64, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i64, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i64, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i64, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i64, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Int8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<i64, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<i64, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i64, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Int16
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i128, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i128, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i128, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i128, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i128, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i128, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i128, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i128, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i128, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i128, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i128, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i128, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Int16(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<i128, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int16(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<i128, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i128, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Uint1
			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u8, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u8, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u8, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u8, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u8, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u8, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u8, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u8, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u8, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u8, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u8, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u8, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Uint1(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<u8, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint1(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<u8, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u8, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Uint2
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u16, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u16, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u16, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u16, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u16, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u16, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u16, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u16, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u16, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u16, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u16, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u16, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Uint2(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<u16, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint2(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<u16, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u16, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Uint4
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u32, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u32, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u32, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u32, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u32, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u32, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u32, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u32, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u32, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u32, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u32, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u32, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Uint4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<u32, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<u32, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u32, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Uint8
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u64, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u64, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u64, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u64, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u64, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u64, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u64, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u64, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u64, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u64, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u64, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u64, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Uint8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<u64, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<u64, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u64, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Uint16
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u128, f32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u128, f64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u128, i8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u128, i16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u128, i32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u128, i64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u128, i128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u128, u8>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u128, u16>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u128, u32>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u128, u64>(ctx, l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u128, u128>(ctx, l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Uint16(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<u128, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint16(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<u128, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u128, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Int
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => Ok(compare_number::<Int, f32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => Ok(compare_number::<Int, f64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => Ok(compare_number::<Int, i8>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => Ok(compare_number::<Int, i16>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => Ok(compare_number::<Int, i32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => Ok(compare_number::<Int, i64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => Ok(compare_number::<Int, i128>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => Ok(compare_number::<Int, u8>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => Ok(compare_number::<Int, u16>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => Ok(compare_number::<Int, u32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => Ok(compare_number::<Int, u64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => Ok(compare_number::<Int, u128>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<Int, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<Int, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<Int, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Uint
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => Ok(compare_number::<Uint, f32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => Ok(compare_number::<Uint, f64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => Ok(compare_number::<Uint, i8>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => Ok(compare_number::<Uint, i16>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => Ok(compare_number::<Uint, i32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => Ok(compare_number::<Uint, i64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => Ok(compare_number::<Uint, i128>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => Ok(compare_number::<Uint, u8>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => Ok(compare_number::<Uint, u16>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => Ok(compare_number::<Uint, u32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => Ok(compare_number::<Uint, u64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => Ok(compare_number::<Uint, u128>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<Uint, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<Uint, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<Uint, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			// Decimal
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => Ok(compare_number::<Decimal, f32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => Ok(compare_number::<Decimal, f64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => Ok(compare_number::<Decimal, i8>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => Ok(compare_number::<Decimal, i16>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => Ok(compare_number::<Decimal, i32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => Ok(compare_number::<Decimal, i64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => Ok(compare_number::<Decimal, i128>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => Ok(compare_number::<Decimal, u8>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => Ok(compare_number::<Decimal, u16>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => Ok(compare_number::<Decimal, u32>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => Ok(compare_number::<Decimal, u64>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => Ok(compare_number::<Decimal, u128>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => Ok(compare_number::<Decimal, Int>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => Ok(compare_number::<Decimal, Uint>(ctx, l, r, eq.full_fragment_owned())),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<Decimal, Decimal>(ctx, l, r, eq.full_fragment_owned())),
			(ColumnData::Date(l), ColumnData::Date(r)) => {
				Ok(compare_temporal(l, r, eq.full_fragment_owned()))
			}
			(ColumnData::DateTime(l), ColumnData::DateTime(r)) => {
				Ok(compare_temporal(l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Time(l), ColumnData::Time(r)) => {
				Ok(compare_temporal(l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Interval(l), ColumnData::Interval(r)) => {
				Ok(compare_temporal(l, r, eq.full_fragment_owned()))
			}
			(
				ColumnData::Utf8 {
					container: l,
					..
				},
				ColumnData::Utf8 {
					container: r,
					..
				},
			) => Ok(compare_utf8(l, r, eq.full_fragment_owned())),
			(ColumnData::Undefined(container), _) | (_, ColumnData::Undefined(container)) => Ok(Column {
				name: eq.full_fragment_owned(),
				data: ColumnData::bool(vec![false; container.len()]),
			}),
			_ => return_error!(equal_cannot_be_applied_to_incompatible_types(
				eq.full_fragment_owned(),
				left.get_type(),
				right.get_type(),
			)),
		}
	}
}

fn compare_bool<'a>(
	ctx: &ColumnEvaluationContext<'a>,
	l: &BoolContainer,
	r: &BoolContainer,
	fragment: Fragment<'_>,
) -> Column<'a> {
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let data: Vec<bool> =
			l.data().iter().zip(r.data().iter()).map(|(l_val, r_val)| l_val == r_val).collect();

		Column {
			name: Fragment::owned_internal(fragment.text()),
			data: ColumnData::bool(data),
		}
	} else {
		// Slow path: some values may be undefined
		let mut data = ctx.pooled(Boolean, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					data.push(l == r);
				}
				_ => data.push_undefined(),
			}
		}

		Column {
			name: Fragment::owned_internal(fragment.text()),
			data,
		}
	}
}

fn compare_number<'a, L, R>(
	ctx: &ColumnEvaluationContext<'a>,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	fragment: Fragment<'_>,
) -> Column<'a>
where
	L: Promote<R> + IsNumber,
	R: IsNumber,
	<L as Promote<R>>::Output: PartialOrd,
{
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let data: Vec<bool> =
			l.data().iter()
				.zip(r.data().iter())
				.map(|(l_val, r_val)| number::is_equal(l_val, r_val))
				.collect();

		Column {
			name: Fragment::owned_internal(fragment.text()),
			data: ColumnData::bool(data),
		}
	} else {
		// Slow path: some values may be undefined
		let mut data = ctx.pooled(Boolean, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					data.push(number::is_equal(l, r));
				}
				_ => data.push_undefined(),
			}
		}

		Column {
			name: Fragment::owned_internal(fragment.text()),
			data,
		}
	}
}

fn compare_temporal<'a, T>(l: &TemporalContainer<T>, r: &TemporalContainer<T>, fragment: Fragment<'_>) -> Column<'a>
where
	T: IsTemporal + Copy,
{
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let data: Vec<bool> =
			l.data().iter()
				.zip(r.data().iter())
				.map(|(l_val, r_val)| temporal::is_equal(l_val, r_val))
				.collect();

		Column {
			name: Fragment::owned_internal(fragment.text()),
			data: ColumnData::bool(data),
		}
	} else {
		// Slow path: some values may be undefined
		let mut data = Vec::with_capacity(l.len());
		let mut bitvec = Vec::with_capacity(l.len());

		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					data.push(temporal::is_equal(l, r));
					bitvec.push(true);
				}
				_ => {
					data.push(false);
					bitvec.push(false);
				}
			}
		}

		Column {
			name: Fragment::owned_internal(fragment.text()),
			data: ColumnData::bool_with_bitvec(data, bitvec),
		}
	}
}

fn compare_utf8<'a>(l: &Utf8Container, r: &Utf8Container, fragment: Fragment<'_>) -> Column<'a> {
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let data: Vec<bool> =
			l.data().iter().zip(r.data().iter()).map(|(l_val, r_val)| l_val == r_val).collect();

		Column {
			name: Fragment::owned_internal(fragment.text()),
			data: ColumnData::bool(data),
		}
	} else {
		// Slow path: some values may be undefined
		let mut data = Vec::with_capacity(l.len());
		let mut bitvec = Vec::with_capacity(l.len());

		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					data.push(l == r);
					bitvec.push(true);
				}
				_ => {
					data.push(false);
					bitvec.push(false);
				}
			}
		}

		Column {
			name: Fragment::owned_internal(fragment.text()),
			data: ColumnData::bool_with_bitvec(data, bitvec),
		}
	}
}
