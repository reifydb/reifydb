// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use Type::Bool;
use reifydb_core::error::diagnostic::operator::equal_cannot_be_applied_to_incompatible_types;
use reifydb_rql::expression::EqualExpression;
use reifydb_core::frame::column::container::{
    BoolContainer, NumberContainer, StringContainer, TemporalContainer,
};
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn};
use reifydb_core::value::number::Promote;
use reifydb_core::value::{IsNumber, IsTemporal, temporal};
use reifydb_core::{OwnedSpan, Type, return_error, value};
use std::fmt::Debug;
use value::number;

impl Evaluator {
    pub(crate) fn equal(
        &mut self,
        eq: &EqualExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&eq.left, ctx)?;
        let right = self.evaluate(&eq.right, ctx)?;

        match (&left.values(), &right.values()) {
            (ColumnValues::Bool(l), ColumnValues::Bool(r)) => {
                Ok(compare_bool(ctx, l, r, eq.span()))
            }
            // Float4
            (ColumnValues::Float4(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<f32, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<f32, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<f32, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<f32, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<f32, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<f32, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<f32, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<f32, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<f32, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<f32, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<f32, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float4(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<f32, u128>(ctx, l, r, eq.span()))
            }
            // Float8
            (ColumnValues::Float8(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<f64, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<f64, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<f64, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<f64, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<f64, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<f64, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<f64, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<f64, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<f64, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<f64, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<f64, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Float8(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<f64, u128>(ctx, l, r, eq.span()))
            }
            // Int1
            (ColumnValues::Int1(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<i8, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<i8, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<i8, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<i8, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<i8, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<i8, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<i8, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<i8, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<i8, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<i8, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<i8, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int1(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<i8, u128>(ctx, l, r, eq.span()))
            }
            // Int2
            (ColumnValues::Int2(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<i16, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<i16, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<i16, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<i16, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<i16, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<i16, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<i16, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<i16, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<i16, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<i16, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<i16, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int2(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<i16, u128>(ctx, l, r, eq.span()))
            }
            // Int4
            (ColumnValues::Int4(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<i32, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<i32, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<i32, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<i32, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<i32, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<i32, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<i32, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<i32, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<i32, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<i32, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<i32, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int4(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<i32, u128>(ctx, l, r, eq.span()))
            }
            // Int8
            (ColumnValues::Int8(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<i64, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<i64, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<i64, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<i64, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<i64, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<i64, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<i64, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<i64, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<i64, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<i64, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<i64, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int8(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<i64, u128>(ctx, l, r, eq.span()))
            }
            // Int16
            (ColumnValues::Int16(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<i128, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<i128, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<i128, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<i128, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<i128, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<i128, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<i128, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<i128, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<i128, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<i128, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<i128, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Int16(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<i128, u128>(ctx, l, r, eq.span()))
            }
            // Uint1
            (ColumnValues::Uint1(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<u8, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<u8, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<u8, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<u8, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<u8, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<u8, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<u8, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<u8, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<u8, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<u8, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<u8, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<u8, u128>(ctx, l, r, eq.span()))
            }
            // Uint2
            (ColumnValues::Uint2(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<u16, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<u16, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<u16, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<u16, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<u16, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<u16, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<u16, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<u16, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<u16, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<u16, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<u16, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<u16, u128>(ctx, l, r, eq.span()))
            }
            // Uint4
            (ColumnValues::Uint4(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<u32, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<u32, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<u32, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<u32, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<u32, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<u32, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<u32, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<u32, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<u32, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<u32, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<u32, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<u32, u128>(ctx, l, r, eq.span()))
            }
            // Uint8
            (ColumnValues::Uint8(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<u64, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<u64, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<u64, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<u64, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<u64, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<u64, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<u64, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<u64, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<u64, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<u64, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<u64, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<u64, u128>(ctx, l, r, eq.span()))
            }
            // Uint16
            (ColumnValues::Uint16(l), ColumnValues::Float4(r)) => {
                Ok(compare_number::<u128, f32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Float8(r)) => {
                Ok(compare_number::<u128, f64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Int1(r)) => {
                Ok(compare_number::<u128, i8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Int2(r)) => {
                Ok(compare_number::<u128, i16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Int4(r)) => {
                Ok(compare_number::<u128, i32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Int8(r)) => {
                Ok(compare_number::<u128, i64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Int16(r)) => {
                Ok(compare_number::<u128, i128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint1(r)) => {
                Ok(compare_number::<u128, u8>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint2(r)) => {
                Ok(compare_number::<u128, u16>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint4(r)) => {
                Ok(compare_number::<u128, u32>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint8(r)) => {
                Ok(compare_number::<u128, u64>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint16(r)) => {
                Ok(compare_number::<u128, u128>(ctx, l, r, eq.span()))
            }
            (ColumnValues::Date(l), ColumnValues::Date(r)) => Ok(compare_temporal(l, r, eq.span())),
            (ColumnValues::DateTime(l), ColumnValues::DateTime(r)) => {
                Ok(compare_temporal(l, r, eq.span()))
            }
            (ColumnValues::Time(l), ColumnValues::Time(r)) => Ok(compare_temporal(l, r, eq.span())),
            (ColumnValues::Interval(l), ColumnValues::Interval(r)) => {
                Ok(compare_temporal(l, r, eq.span()))
            }
            (ColumnValues::Utf8(l), ColumnValues::Utf8(r)) => Ok(compare_utf8(l, r, eq.span())),
            (ColumnValues::Undefined(container), _) | (_, ColumnValues::Undefined(container)) => {
                let span = eq.span();
                Ok(FrameColumn::ColumnQualified(ColumnQualified {
                    name: span.fragment.into(),
                    values: ColumnValues::bool(vec![false; container.len()]),
                }))
            }
            _ => return_error!(equal_cannot_be_applied_to_incompatible_types(
                eq.span(),
                left.get_type(),
                right.get_type(),
            )),
        }
    }
}

fn compare_bool(
    ctx: &EvaluationContext,
    l: &BoolContainer,
    r: &BoolContainer,
    span: OwnedSpan,
) -> FrameColumn {
    debug_assert_eq!(l.len(), r.len());

    let mut values = ctx.pooled_values(Bool, l.len());

    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                values.push(l == r);
            }
            _ => values.push_undefined(),
        }
    }

    FrameColumn::ColumnQualified(ColumnQualified { name: span.fragment.into(), values })
}

fn compare_number<L, R>(
    ctx: &EvaluationContext,
    l: &NumberContainer<L>,
    r: &NumberContainer<R>,
    span: OwnedSpan,
) -> FrameColumn
where
    L: Promote<R> + IsNumber + Clone + Debug + Default,
    R: IsNumber + Copy + Clone + Debug + Default,
    <L as Promote<R>>::Output: PartialOrd,
{
    debug_assert_eq!(l.len(), r.len());

    let mut values = ctx.pooled_values(Bool, l.len());

    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                values.push(number::is_equal(*l, *r));
            }
            _ => values.push_undefined(),
        }
    }

    FrameColumn::ColumnQualified(ColumnQualified { name: span.fragment.into(), values })
}

fn compare_temporal<T>(
    l: &TemporalContainer<T>,
    r: &TemporalContainer<T>,
    span: OwnedSpan,
) -> FrameColumn
where
    T: IsTemporal + Clone + Debug + Default,
{
    debug_assert_eq!(l.len(), r.len());

    let mut values = Vec::with_capacity(l.len());
    let mut bitvec = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                values.push(temporal::is_equal(*l, *r));
                bitvec.push(true);
            }
            _ => {
                values.push(false);
                bitvec.push(false);
            }
        }
    }

    FrameColumn::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        values: ColumnValues::bool_with_bitvec(values, bitvec),
    })
}

fn compare_utf8(l: &StringContainer, r: &StringContainer, span: OwnedSpan) -> FrameColumn {
    debug_assert_eq!(l.len(), r.len());

    let mut values = Vec::with_capacity(l.len());
    let mut bitvec = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                values.push(l == r);
                bitvec.push(true);
            }
            _ => {
                values.push(false);
                bitvec.push(false);
            }
        }
    }
    FrameColumn::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        values: ColumnValues::bool_with_bitvec(values, bitvec),
    })
}
