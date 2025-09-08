// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Evaluator, evaluate::expression::EqExpression},
	return_error,
	value::{
		columnar::{Column, ColumnData, ColumnQualified},
		container::{
			BoolContainer, NumberContainer, TemporalContainer,
			Utf8Container,
		},
	},
};
use reifydb_type::{
	Decimal, Fragment, IsNumber, IsTemporal, Promote, Type::Boolean,
	VarInt, VarUint,
	diagnostic::operator::equal_cannot_be_applied_to_incompatible_types,
	temporal, value::number,
};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn equal(
		&self,
		ctx: &EvaluationContext,
		eq: &EqExpression,
	) -> crate::Result<Column> {
		let left = self.evaluate(ctx, &eq.left)?;
		let right = self.evaluate(ctx, &eq.right)?;

		match (&left.data(), &right.data()) {
			(ColumnData::Bool(l), ColumnData::Bool(r)) => {
				Ok(compare_bool(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			// Float4 with VarInt, VarUint, Decimal
			(ColumnData::Float4(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<f32, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<f32, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Float4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<f32, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Float4
			(ColumnData::Float4(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<f32, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<f32, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<f32, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<f32, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<f32, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<f32, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<f32, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<f32, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<f32, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<f32, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<f32, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<f32, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<f64, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<f64, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<f64, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<f64, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<f64, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<f64, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<f64, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<f64, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<f64, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<f64, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<f64, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<f64, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<f64, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Float8(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<f64, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Float8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<f64, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Int1
			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i8, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i8, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i8, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i8, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i8, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i8, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i8, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i8, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i8, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i8, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i8, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i8, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<i8, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int1(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<i8, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Int1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i8, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Int2
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i16, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i16, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i16, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i16, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i16, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i16, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i16, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i16, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i16, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i16, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i16, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i16, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<i16, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int2(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<i16, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Int2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i16, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Int4
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i32, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i32, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i32, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i32, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i32, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i32, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i32, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i32, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i32, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i32, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i32, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i32, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<i32, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int4(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<i32, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Int4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i32, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Int8
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i64, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i64, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i64, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i64, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i64, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i64, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i64, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i64, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i64, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i64, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i64, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i64, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<i64, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int8(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<i64, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Int8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i64, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Int16
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<i128, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<i128, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<i128, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<i128, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<i128, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<i128, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<i128, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<i128, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<i128, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<i128, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<i128, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<i128, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<i128, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Int16(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<i128, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Int16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<i128, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Uint1
			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u8, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u8, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u8, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u8, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u8, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u8, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u8, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u8, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u8, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u8, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u8, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u8, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<u8, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint1(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<u8, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Uint1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u8, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Uint2
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u16, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u16, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u16, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u16, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u16, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u16, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u16, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u16, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u16, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u16, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u16, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u16, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<u16, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint2(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<u16, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Uint2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u16, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Uint4
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u32, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u32, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u32, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u32, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u32, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u32, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u32, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u32, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u32, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u32, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u32, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u32, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<u32, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint4(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<u32, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Uint4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u32, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Uint8
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u64, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u64, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u64, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u64, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u64, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u64, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u64, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u64, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u64, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u64, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u64, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u64, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<u64, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint8(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<u64, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Uint8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u64, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Uint16
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<u128, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<u128, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<u128, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<u128, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<u128, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<u128, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<u128, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<u128, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<u128, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<u128, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<u128, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<u128, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<u128, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Uint16(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<u128, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::Uint16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<u128, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// VarInt
			(ColumnData::VarInt(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<VarInt, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<VarInt, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<VarInt, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<VarInt, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<VarInt, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<VarInt, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<VarInt, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<VarInt, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<VarInt, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<VarInt, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<VarInt, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<VarInt, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<VarInt, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarInt(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<VarInt, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::VarInt(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<VarInt, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// VarUint
			(ColumnData::VarUint(l), ColumnData::Float4(r)) => {
				Ok(compare_number::<VarUint, f32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Float8(r)) => {
				Ok(compare_number::<VarUint, f64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Int1(r)) => {
				Ok(compare_number::<VarUint, i8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Int2(r)) => {
				Ok(compare_number::<VarUint, i16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Int4(r)) => {
				Ok(compare_number::<VarUint, i32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Int8(r)) => {
				Ok(compare_number::<VarUint, i64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Int16(r)) => {
				Ok(compare_number::<VarUint, i128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Uint1(r)) => {
				Ok(compare_number::<VarUint, u8>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Uint2(r)) => {
				Ok(compare_number::<VarUint, u16>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Uint4(r)) => {
				Ok(compare_number::<VarUint, u32>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Uint8(r)) => {
				Ok(compare_number::<VarUint, u64>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::Uint16(r)) => {
				Ok(compare_number::<VarUint, u128>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::VarInt(r)) => {
				Ok(compare_number::<VarUint, VarInt>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::VarUint(l), ColumnData::VarUint(r)) => {
				Ok(compare_number::<VarUint, VarUint>(
					ctx,
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(
				ColumnData::VarUint(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<VarUint, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			// Decimal
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => Ok(compare_number::<Decimal, f32>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => Ok(compare_number::<Decimal, f64>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => Ok(compare_number::<Decimal, i8>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => Ok(compare_number::<Decimal, i16>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => Ok(compare_number::<Decimal, i32>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => Ok(compare_number::<Decimal, i64>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => Ok(compare_number::<Decimal, i128>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => Ok(compare_number::<Decimal, u8>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => Ok(compare_number::<Decimal, u16>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => Ok(compare_number::<Decimal, u32>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => Ok(compare_number::<Decimal, u64>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => Ok(compare_number::<Decimal, u128>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::VarInt(r),
			) => Ok(compare_number::<Decimal, VarInt>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::VarUint(r),
			) => Ok(compare_number::<Decimal, VarUint>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => Ok(compare_number::<Decimal, Decimal>(
				ctx,
				l,
				r,
				eq.full_fragment_owned(),
			)),
			(ColumnData::Date(l), ColumnData::Date(r)) => {
				Ok(compare_temporal(
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::DateTime(l), ColumnData::DateTime(r)) => {
				Ok(compare_temporal(
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Time(l), ColumnData::Time(r)) => {
				Ok(compare_temporal(
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Interval(l), ColumnData::Interval(r)) => {
				Ok(compare_temporal(
					l,
					r,
					eq.full_fragment_owned(),
				))
			}
			(ColumnData::Utf8(l), ColumnData::Utf8(r)) => {
				Ok(compare_utf8(l, r, eq.full_fragment_owned()))
			}
			(ColumnData::Undefined(container), _)
			| (_, ColumnData::Undefined(container)) => {
				let fragment = eq.full_fragment_owned();
				Ok(Column::ColumnQualified(ColumnQualified {
					name: fragment.fragment().into(),
					data: ColumnData::bool(vec![
						false;
						container
							.len(
							)
					]),
				}))
			}
			_ => return_error!(
				equal_cannot_be_applied_to_incompatible_types(
					eq.full_fragment_owned(),
					left.get_type(),
					right.get_type(),
				)
			),
		}
	}
}

fn compare_bool(
	ctx: &EvaluationContext,
	l: &BoolContainer,
	r: &BoolContainer,
	fragment: Fragment<'_>,
) -> Column {
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(Boolean, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				data.push(l == r);
			}
			_ => data.push_undefined(),
		}
	}

	Column::ColumnQualified(ColumnQualified {
		name: fragment.fragment().into(),
		data,
	})
}

fn compare_number<L, R>(
	ctx: &EvaluationContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	fragment: Fragment<'_>,
) -> Column
where
	L: Promote<R> + IsNumber,
	R: IsNumber,
	<L as Promote<R>>::Output: PartialOrd,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(Boolean, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				data.push(number::is_equal(l, r));
			}
			_ => data.push_undefined(),
		}
	}

	Column::ColumnQualified(ColumnQualified {
		name: fragment.fragment().into(),
		data,
	})
}

fn compare_temporal<T>(
	l: &TemporalContainer<T>,
	r: &TemporalContainer<T>,
	fragment: Fragment<'_>,
) -> Column
where
	T: IsTemporal + Copy,
{
	debug_assert_eq!(l.len(), r.len());

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

	Column::ColumnQualified(ColumnQualified {
		name: fragment.fragment().into(),
		data: ColumnData::bool_with_bitvec(data, bitvec),
	})
}

fn compare_utf8(
	l: &Utf8Container,
	r: &Utf8Container,
	fragment: Fragment<'_>,
) -> Column {
	debug_assert_eq!(l.len(), r.len());

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
	Column::ColumnQualified(ColumnQualified {
		name: fragment.fragment().into(),
		data: ColumnData::bool_with_bitvec(data, bitvec),
	})
}
