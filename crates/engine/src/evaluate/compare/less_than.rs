// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::{Column, ColumnValues};
use reifydb_core::num::{IsNumber, Promote, is_less_than};
use reifydb_core::{CowVec, Span};
use reifydb_rql::expression::LessThanExpression;

impl Evaluator {
    pub(crate) fn less_than(
        &mut self,
        lt: &LessThanExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<Column> {
        let left = self.evaluate(&lt.left, ctx)?;
        let right = self.evaluate(&lt.right, ctx)?;

        match (&left.data, &right.data) {
            // Float4
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float4(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<f32, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Float8
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Float8(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<f64, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Int1
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i8, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Int2
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i16, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Int4
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i32, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Int8
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i64, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Int16
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<i128, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Uint1
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint1(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u8, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Uint2
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint2(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u16, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Uint4
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint4(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u32, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Uint8
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint8(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u64, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            // Uint16
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Float4(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, f32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Float8(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, f64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int8(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Int16(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, i128>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint1(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u8>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint2(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u16>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint4(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u32>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint8(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u64>(lv, rv, lv_valid, rv_valid, lt.span()))
            }
            (ColumnValues::Uint16(lv, lv_valid), ColumnValues::Uint16(rv, rv_valid)) => {
                Ok(compare_numeric::<u128, u128>(lv, rv, lv_valid, rv_valid, lt.span()))
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
) -> Column
where
    L: Promote<R> + Copy,
    R: IsNumber + Copy,
    <L as Promote<R>>::Output: PartialOrd,
{
    let mut values = Vec::with_capacity(l.len());
    let mut valid = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        if lv[i] && rv[i] {
            values.push(is_less_than(l[i], r[i]));
            valid.push(true);
        } else {
            values.push(false);
            valid.push(false);
        }
    }

    Column { name: span.fragment, data: ColumnValues::bool_with_validity(values, valid) }
}
