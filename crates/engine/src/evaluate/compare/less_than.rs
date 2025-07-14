// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::{FrameColumn, ColumnValues};
use reifydb_core::num::{IsNumber, Promote, is_less_than};
use reifydb_core::{CowVec, Span};
use reifydb_rql::expression::LessThanExpression;

impl Evaluator {
    pub(crate) fn less_than(
		&mut self,
		lt: &LessThanExpression,
		ctx: &EvaluationContext,
    ) -> crate::evaluate::Result<FrameColumn> {
        let left = self.evaluate(&lt.left, ctx)?;
        let right = self.evaluate(&lt.right, ctx)?;

        match (&left.values, &right.values) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<f32, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<f32, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<f32, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<f32, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<f32, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<f32, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<f32, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<f32, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<f32, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<f32, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<f32, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<f32, u128>(l, r, lv, rv, lt.span()))
            }
            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<f64, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<f64, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<f64, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<f64, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<f64, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<f64, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<f64, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<f64, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<f64, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<f64, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<f64, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<f64, u128>(l, r, lv, rv, lt.span()))
            }
            // Int1
            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<i8, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<i8, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<i8, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<i8, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<i8, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<i8, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<i8, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<i8, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<i8, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<i8, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<i8, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<i8, u128>(l, r, lv, rv, lt.span()))
            }
            // Int2
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<i16, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<i16, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<i16, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<i16, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<i16, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<i16, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<i16, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<i16, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<i16, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<i16, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<i16, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<i16, u128>(l, r, lv, rv, lt.span()))
            }
            // Int4
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<i32, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<i32, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<i32, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<i32, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<i32, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<i32, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<i32, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<i32, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<i32, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<i32, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<i32, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<i32, u128>(l, r, lv, rv, lt.span()))
            }
            // Int8
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<i64, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<i64, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<i64, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<i64, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<i64, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<i64, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<i64, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<i64, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<i64, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<i64, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<i64, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<i64, u128>(l, r, lv, rv, lt.span()))
            }
            // Int16
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<i128, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<i128, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<i128, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<i128, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<i128, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<i128, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<i128, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<i128, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<i128, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<i128, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<i128, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<i128, u128>(l, r, lv, rv, lt.span()))
            }
            // Uint1
            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<u8, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<u8, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<u8, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<u8, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<u8, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<u8, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<u8, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<u8, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<u8, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<u8, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<u8, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<u8, u128>(l, r, lv, rv, lt.span()))
            }
            // Uint2
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<u16, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<u16, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<u16, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<u16, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<u16, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<u16, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<u16, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<u16, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<u16, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<u16, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<u16, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<u16, u128>(l, r, lv, rv, lt.span()))
            }
            // Uint4
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<u32, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<u32, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<u32, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<u32, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<u32, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<u32, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<u32, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<u32, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<u32, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<u32, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<u32, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<u32, u128>(l, r, lv, rv, lt.span()))
            }
            // Uint8
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<u64, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<u64, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<u64, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<u64, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<u64, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<u64, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<u64, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<u64, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<u64, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<u64, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<u64, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<u64, u128>(l, r, lv, rv, lt.span()))
            }
            // Uint16
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_numeric::<u128, f32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_numeric::<u128, f64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_numeric::<u128, i8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_numeric::<u128, i16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_numeric::<u128, i32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_numeric::<u128, i64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_numeric::<u128, i128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_numeric::<u128, u8>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_numeric::<u128, u16>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_numeric::<u128, u32>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_numeric::<u128, u64>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_numeric::<u128, u128>(l, r, lv, rv, lt.span()))
            }
            (ColumnValues::Utf8(l, lv), ColumnValues::Utf8(r, rv)) => {
                Ok(compare_utf8(l, r, lv, rv, lt.span()))
            }
            _ => unimplemented!(),
        }
    }
}

pub fn compare_numeric<L, R>(
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
    let mut valid = Vec::with_capacity(lv.len());

    for i in 0..l.len() {
        if lv[i] && rv[i] {
            values.push(is_less_than(l[i], r[i]));
            valid.push(true);
        } else {
            values.push(false);
            valid.push(false);
        }
    }

    FrameColumn { name: span.fragment, values: ColumnValues::bool_with_validity(values, valid) }
}

fn compare_utf8(
    l: &CowVec<String>,
    r: &CowVec<String>,
    lv: &CowVec<bool>,
    rv: &CowVec<bool>,
    span: Span,
) -> FrameColumn {
    let mut values = Vec::with_capacity(l.len());
    let mut valid = Vec::with_capacity(lv.len());

    for i in 0..l.len() {
        if lv[i] && rv[i] {
            values.push(l[i] <= r[i]);
            valid.push(true);
        } else {
            values.push(false);
            valid.push(false);
        }
    }

    FrameColumn { name: span.fragment, values: ColumnValues::bool_with_validity(values, valid) }
}
