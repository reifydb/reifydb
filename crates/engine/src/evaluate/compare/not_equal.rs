// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::Type::Bool;
use reifydb_core::error::diagnostic::operator::not_equal_cannot_be_applied_to_incompatible_types;
use reifydb_core::expression::NotEqualExpression;
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn};
use reifydb_core::value::number::Promote;
use reifydb_core::value::{IsNumber, IsTemporal, temporal};
use reifydb_core::{BitVec, CowVec, OwnedSpan, return_error, value};

impl Evaluator {
    pub(crate) fn not_equal(
        &mut self,
        ne: &NotEqualExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&ne.left, ctx)?;
        let right = self.evaluate(&ne.right, ctx)?;

        match (&left.values(), &right.values()) {
            (ColumnValues::Bool(l, lv), ColumnValues::Bool(r, rv)) => {
                Ok(compare_bool(ctx, l, r, lv, rv, ne.span()))
            }
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<f32, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<f32, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<f32, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<f32, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<f32, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<f32, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<f32, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<f32, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<f32, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<f32, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<f32, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<f32, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<f64, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<f64, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<f64, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<f64, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<f64, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<f64, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<f64, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<f64, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<f64, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<f64, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<f64, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<f64, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Int1
            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<i8, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<i8, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<i8, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<i8, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<i8, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<i8, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<i8, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<i8, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<i8, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<i8, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<i8, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<i8, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Int2
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<i16, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<i16, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<i16, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<i16, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<i16, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<i16, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<i16, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<i16, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<i16, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<i16, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<i16, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<i16, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Int4
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<i32, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<i32, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<i32, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<i32, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<i32, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<i32, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<i32, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<i32, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<i32, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<i32, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<i32, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<i32, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Int8
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<i64, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<i64, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<i64, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<i64, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<i64, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<i64, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<i64, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<i64, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<i64, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<i64, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<i64, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<i64, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Int16
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<i128, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<i128, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<i128, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<i128, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<i128, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<i128, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<i128, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<i128, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<i128, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<i128, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<i128, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<i128, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Uint1
            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<u8, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<u8, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<u8, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<u8, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<u8, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<u8, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<u8, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<u8, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<u8, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<u8, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<u8, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<u8, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Uint2
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<u16, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<u16, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<u16, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<u16, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<u16, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<u16, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<u16, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<u16, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<u16, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<u16, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<u16, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<u16, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Uint4
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<u32, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<u32, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<u32, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<u32, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<u32, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<u32, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<u32, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<u32, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<u32, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<u32, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<u32, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<u32, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Uint8
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<u64, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<u64, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<u64, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<u64, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<u64, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<u64, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<u64, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<u64, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<u64, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<u64, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<u64, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<u64, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            // Uint16
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                Ok(compare_number::<u128, f32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                Ok(compare_number::<u128, f64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                Ok(compare_number::<u128, i8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                Ok(compare_number::<u128, i16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                Ok(compare_number::<u128, i32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                Ok(compare_number::<u128, i64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                Ok(compare_number::<u128, i128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                Ok(compare_number::<u128, u8>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                Ok(compare_number::<u128, u16>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                Ok(compare_number::<u128, u32>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                Ok(compare_number::<u128, u64>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                Ok(compare_number::<u128, u128>(ctx, l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Date(l, lv), ColumnValues::Date(r, rv)) => {
                Ok(compare_temporal(l, r, lv, rv, ne.span()))
            }
            (ColumnValues::DateTime(l, lv), ColumnValues::DateTime(r, rv)) => {
                Ok(compare_temporal(l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Time(l, lv), ColumnValues::Time(r, rv)) => {
                Ok(compare_temporal(l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Interval(l, lv), ColumnValues::Interval(r, rv)) => {
                Ok(compare_temporal(l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Utf8(l, lv), ColumnValues::Utf8(r, rv)) => {
                Ok(compare_utf8(l, r, lv, rv, ne.span()))
            }
            (ColumnValues::Undefined(size), _) | (_, ColumnValues::Undefined(size)) => {
                let span = ne.span();
                Ok(FrameColumn::ColumnQualified(ColumnQualified {
                    name: span.fragment.into(),
                    values: ColumnValues::bool(vec![false; *size]),
                }))
            }
            _ => return_error!(not_equal_cannot_be_applied_to_incompatible_types(
                ne.span(),
                left.get_type(),
                right.get_type(),
            )),
        }
    }
}

fn compare_bool(
    ctx: &EvaluationContext,
    l: &BitVec,
    r: &BitVec,
    lv: &BitVec,
    rv: &BitVec,
    span: OwnedSpan,
) -> FrameColumn {
    debug_assert_eq!(l.len(), r.len());
    debug_assert_eq!(lv.len(), rv.len());
    debug_assert_eq!(l.len(), lv.len());

    let mut values = ctx.pooled_values(Bool, l.len());

    for i in 0..l.len() {
        if lv.get(i) && rv.get(i) {
            values.push(l.get(i) != r.get(i));
        } else {
            values.push_undefined();
        }
    }

    FrameColumn::ColumnQualified(ColumnQualified { name: span.fragment.into(), values })
}

fn compare_number<L, R>(
    ctx: &EvaluationContext,
    l: &CowVec<L>,
    r: &CowVec<R>,
    lv: &BitVec,
    rv: &BitVec,
    span: OwnedSpan,
) -> FrameColumn
where
    L: Promote<R> + IsNumber,
    R: IsNumber + Copy,
    <L as Promote<R>>::Output: PartialOrd,
{
    debug_assert_eq!(l.len(), r.len());
    debug_assert_eq!(lv.len(), rv.len());
    debug_assert_eq!(l.len(), lv.len());

    let mut values = ctx.pooled_values(Bool, l.len());

    for i in 0..l.len() {
        if lv.get(i) && rv.get(i) {
            values.push(value::number::is_not_equal(l[i], r[i]));
        } else {
            values.push_undefined();
        }
    }

    FrameColumn::ColumnQualified(ColumnQualified { name: span.fragment.into(), values })
}

fn compare_temporal<T>(
    l: &CowVec<T>,
    r: &CowVec<T>,
    lv: &BitVec,
    rv: &BitVec,
    span: OwnedSpan,
) -> FrameColumn
where
    T: IsTemporal,
{
    debug_assert_eq!(l.len(), r.len());
    debug_assert_eq!(lv.len(), rv.len());

    let mut values = Vec::with_capacity(l.len());
    let mut bitvec = Vec::with_capacity(lv.len());

    for i in 0..l.len() {
        if lv.get(i) && rv.get(i) {
            values.push(temporal::is_not_equal(l[i], r[i]));
            bitvec.push(true);
        } else {
            values.push(false);
            bitvec.push(false);
        }
    }

    FrameColumn::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        values: ColumnValues::bool_with_bitvec(values, bitvec),
    })
}

fn compare_utf8(
    l: &CowVec<String>,
    r: &CowVec<String>,
    lv: &BitVec,
    rv: &BitVec,
    span: OwnedSpan,
) -> FrameColumn {
    debug_assert_eq!(l.len(), r.len());
    debug_assert_eq!(lv.len(), rv.len());

    let mut values = Vec::with_capacity(l.len());
    let mut bitvec = Vec::with_capacity(lv.len());

    for i in 0..l.len() {
        if lv.get(i) && rv.get(i) {
            values.push(l.get(i) != r.get(i));
            bitvec.push(true);
        } else {
            values.push(false);
            bitvec.push(false);
        }
    }

    FrameColumn::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        values: ColumnValues::bool_with_bitvec(values, bitvec),
    })
}
