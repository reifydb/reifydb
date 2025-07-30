// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::container::number::NumberContainer;
use crate::column::container::string::StringContainer;
use crate::column::container::temporal::TemporalContainer;
use crate::column::{ColumnQualified, EngineColumn, EngineColumnData};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::Type::Bool;
use reifydb_core::error::diagnostic::operator::greater_than_equal_cannot_be_applied_to_incompatible_types;
use reifydb_core::value::number::Promote;
use reifydb_core::value::{IsNumber, IsTemporal, temporal};
use reifydb_core::{OwnedSpan, return_error, value};
use reifydb_rql::expression::GreaterThanEqualExpression;
use std::fmt::Debug;

impl Evaluator {
    pub(crate) fn greater_than_equal(
        &mut self,
        gte: &GreaterThanEqualExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<EngineColumn> {
        let left = self.evaluate(&gte.left, ctx)?;
        let right = self.evaluate(&gte.right, ctx)?;

        match (&left.data(), &right.data()) {
            // Float4
            (EngineColumnData::Float4(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<f32, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<f32, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<f32, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<f32, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<f32, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<f32, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<f32, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<f32, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<f32, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<f32, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<f32, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<f32, u128>(ctx, l, r, gte.span()))
            }
            // Float8
            (EngineColumnData::Float8(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<f64, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<f64, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<f64, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<f64, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<f64, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<f64, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<f64, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<f64, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<f64, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<f64, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<f64, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<f64, u128>(ctx, l, r, gte.span()))
            }
            // Int1
            (EngineColumnData::Int1(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<i8, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<i8, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<i8, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<i8, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<i8, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<i8, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<i8, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<i8, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<i8, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<i8, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<i8, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<i8, u128>(ctx, l, r, gte.span()))
            }
            // Int2
            (EngineColumnData::Int2(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<i16, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<i16, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<i16, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<i16, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<i16, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<i16, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<i16, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<i16, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<i16, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<i16, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<i16, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<i16, u128>(ctx, l, r, gte.span()))
            }
            // Int4
            (EngineColumnData::Int4(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<i32, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<i32, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<i32, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<i32, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<i32, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<i32, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<i32, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<i32, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<i32, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<i32, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<i32, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<i32, u128>(ctx, l, r, gte.span()))
            }
            // Int8
            (EngineColumnData::Int8(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<i64, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<i64, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<i64, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<i64, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<i64, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<i64, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<i64, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<i64, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<i64, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<i64, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<i64, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<i64, u128>(ctx, l, r, gte.span()))
            }
            // Int16
            (EngineColumnData::Int16(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<i128, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<i128, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<i128, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<i128, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<i128, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<i128, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<i128, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<i128, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<i128, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<i128, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<i128, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<i128, u128>(ctx, l, r, gte.span()))
            }
            // Uint1
            (EngineColumnData::Uint1(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<u8, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<u8, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<u8, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<u8, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<u8, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<u8, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<u8, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<u8, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<u8, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<u8, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<u8, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<u8, u128>(ctx, l, r, gte.span()))
            }
            // Uint2
            (EngineColumnData::Uint2(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<u16, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<u16, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<u16, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<u16, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<u16, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<u16, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<u16, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<u16, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<u16, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<u16, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<u16, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<u16, u128>(ctx, l, r, gte.span()))
            }
            // Uint4
            (EngineColumnData::Uint4(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<u32, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<u32, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<u32, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<u32, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<u32, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<u32, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<u32, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<u32, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<u32, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<u32, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<u32, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<u32, u128>(ctx, l, r, gte.span()))
            }
            // Uint8
            (EngineColumnData::Uint8(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<u64, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<u64, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<u64, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<u64, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<u64, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<u64, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<u64, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<u64, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<u64, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<u64, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<u64, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<u64, u128>(ctx, l, r, gte.span()))
            }
            // Uint16
            (EngineColumnData::Uint16(l), EngineColumnData::Float4(r)) => {
                Ok(compare_number::<u128, f32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Float8(r)) => {
                Ok(compare_number::<u128, f64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int1(r)) => {
                Ok(compare_number::<u128, i8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int2(r)) => {
                Ok(compare_number::<u128, i16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int4(r)) => {
                Ok(compare_number::<u128, i32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int8(r)) => {
                Ok(compare_number::<u128, i64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int16(r)) => {
                Ok(compare_number::<u128, i128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint1(r)) => {
                Ok(compare_number::<u128, u8>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint2(r)) => {
                Ok(compare_number::<u128, u16>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint4(r)) => {
                Ok(compare_number::<u128, u32>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint8(r)) => {
                Ok(compare_number::<u128, u64>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint16(r)) => {
                Ok(compare_number::<u128, u128>(ctx, l, r, gte.span()))
            }
            (EngineColumnData::Date(l), EngineColumnData::Date(r)) => {
                Ok(compare_temporal(l, r, gte.span()))
            }
            (EngineColumnData::DateTime(l), EngineColumnData::DateTime(r)) => {
                Ok(compare_temporal(l, r, gte.span()))
            }
            (EngineColumnData::Time(l), EngineColumnData::Time(r)) => {
                Ok(compare_temporal(l, r, gte.span()))
            }
            (EngineColumnData::Interval(l), EngineColumnData::Interval(r)) => {
                Ok(compare_temporal(l, r, gte.span()))
            }
            (EngineColumnData::Utf8(l), EngineColumnData::Utf8(r)) => {
                Ok(compare_utf8(l, r, gte.span()))
            }
            (EngineColumnData::Undefined(container), _)
            | (_, EngineColumnData::Undefined(container)) => {
                let span = gte.span();
                Ok(EngineColumn::ColumnQualified(ColumnQualified {
                    name: span.fragment.into(),
                    data: EngineColumnData::bool(vec![false; container.len()]),
                }))
            }
            _ => return_error!(greater_than_equal_cannot_be_applied_to_incompatible_types(
                gte.span(),
                left.get_type(),
                right.get_type(),
            )),
        }
    }
}

pub fn compare_number<L, R>(
    ctx: &EvaluationContext,
    l: &NumberContainer<L>,
    r: &NumberContainer<R>,
    span: OwnedSpan,
) -> EngineColumn
where
    L: Promote<R> + IsNumber + Clone + Debug + Default,
    R: IsNumber + Copy + Clone + Debug + Default,
    <L as Promote<R>>::Output: PartialOrd,
{
    debug_assert_eq!(l.len(), r.len());

    let mut data = ctx.pooled(Bool, l.len());
    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                data.push(value::number::is_greater_than_equal(*l, *r));
            }
            _ => data.push_undefined(),
        }
    }

    EngineColumn::ColumnQualified(ColumnQualified { name: span.fragment.into(), data })
}

fn compare_temporal<T>(
    l: &TemporalContainer<T>,
    r: &TemporalContainer<T>,
    span: OwnedSpan,
) -> EngineColumn
where
    T: IsTemporal + Clone + Debug + Default,
{
    debug_assert_eq!(l.len(), r.len());

    let mut data = Vec::with_capacity(l.len());
    let mut bitvec = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                data.push(temporal::is_greater_than_equal(*l, *r));
                bitvec.push(true);
            }
            _ => {
                data.push(false);
                bitvec.push(false);
            }
        }
    }

    EngineColumn::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        data: EngineColumnData::bool_with_bitvec(data, bitvec),
    })
}

fn compare_utf8(l: &StringContainer, r: &StringContainer, span: OwnedSpan) -> EngineColumn {
    debug_assert_eq!(l.len(), r.len());

    let mut data = Vec::with_capacity(l.len());
    let mut bitvec = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                data.push(*l >= *r);
                bitvec.push(true);
            }
            _ => {
                data.push(false);
                bitvec.push(false);
            }
        }
    }

    EngineColumn::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        data: EngineColumnData::bool_with_bitvec(data, bitvec),
    })
}
