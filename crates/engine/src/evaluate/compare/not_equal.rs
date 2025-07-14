// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::{FrameColumn, ColumnValues};
use reifydb_core::num::{IsNumber, Promote, is_not_equal};
use reifydb_core::{CowVec, Span};
use reifydb_rql::expression::NotEqualExpression;

impl Evaluator {
    pub(crate) fn not_equal(
		&mut self,
		ne: &NotEqualExpression,
		ctx: &EvaluationContext,
    ) -> crate::evaluate::Result<FrameColumn> {
        let left = self.evaluate(&ne.left, ctx)?;
        let right = self.evaluate(&ne.right, ctx)?;

        match (&left.values, &right.values) {
            (ColumnValues::Bool(lv, lv_valid), ColumnValues::Bool(rv, rv_valid)) => {
                Ok(compare_bool(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Float4
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Float8
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Int1
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Int2
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Int4
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Int8
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Int16
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Uint1
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Uint2
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Uint4
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Uint8
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            // Uint16
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, f32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, f64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u8>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u16>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u32>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u64>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u128>(lv, rv, lv_valid, rv_valid, ne.span()))
            }
            _ => unimplemented!(),
        }
    }
}

fn compare_bool(
    l: &CowVec<bool>,
    r: &CowVec<bool>,
    lv: &CowVec<bool>,
    rv: &CowVec<bool>,
    span: Span,
) -> FrameColumn {
    let mut values = Vec::with_capacity(l.len());
    let mut valid = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        if lv[i] && rv[i] {
            values.push(l[i] != r[i]);
            valid.push(true);
        } else {
            values.push(false);
            valid.push(false);
        }
    }

    FrameColumn { name: span.fragment, values: ColumnValues::bool_with_validity(values, valid) }
}

fn compare_numeric<L, R>(
    l: &CowVec<L>,
    r: &CowVec<R>,
    lv: &CowVec<bool>,
    rv: &CowVec<bool>,
    span: Span,
) -> FrameColumn
where
    L: Promote<R> + Copy,
    R: IsNumber + Copy,
    <L as Promote<R>>::Output: PartialOrd,
{
    let mut values = Vec::with_capacity(l.len());
    let mut valid = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        if lv[i] && rv[i] {
            values.push(is_not_equal(l[i], r[i]));
            valid.push(true);
        } else {
            values.push(false);
            valid.push(false);
        }
    }

    FrameColumn { name: span.fragment, values: ColumnValues::bool_with_validity(values, valid) }
}
