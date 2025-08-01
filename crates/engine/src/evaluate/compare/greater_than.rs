// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{Column, ColumnData, ColumnQualified};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::Type::Bool;
use reifydb_core::result::error::diagnostic::operator::greater_than_cannot_be_applied_to_incompatible_types;
use reifydb_core::value::container::number::NumberContainer;
use reifydb_core::value::container::string::StringContainer;
use reifydb_core::value::container::temporal::TemporalContainer;
use reifydb_core::value::number::Promote;
use reifydb_core::value::{IsNumber, IsTemporal, temporal};
use reifydb_core::{OwnedSpan, return_error, value};
use reifydb_rql::expression::GreaterThanExpression;
use std::fmt::Debug;

impl Evaluator {
    pub(crate) fn greater_than(
        &mut self,
        gt: &GreaterThanExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let left = self.evaluate(&gt.left, ctx)?;
        let right = self.evaluate(&gt.right, ctx)?;

        match (&left.data(), &right.data()) {
            // Float4
            (ColumnData::Float4(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<f32, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<f32, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<f32, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<f32, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<f32, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<f32, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<f32, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<f32, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<f32, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<f32, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<f32, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float4(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<f32, u128>(ctx, l, r, gt.span()))
            }
            // Float8
            (ColumnData::Float8(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<f64, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<f64, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<f64, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<f64, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<f64, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<f64, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<f64, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<f64, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<f64, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<f64, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<f64, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Float8(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<f64, u128>(ctx, l, r, gt.span()))
            }
            // Int1
            (ColumnData::Int1(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<i8, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<i8, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<i8, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<i8, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<i8, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<i8, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<i8, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<i8, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<i8, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<i8, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<i8, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int1(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<i8, u128>(ctx, l, r, gt.span()))
            }
            // Int2
            (ColumnData::Int2(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<i16, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<i16, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<i16, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<i16, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<i16, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<i16, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<i16, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<i16, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<i16, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<i16, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<i16, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int2(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<i16, u128>(ctx, l, r, gt.span()))
            }
            // Int4
            (ColumnData::Int4(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<i32, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<i32, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<i32, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<i32, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<i32, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<i32, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<i32, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<i32, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<i32, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<i32, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<i32, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int4(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<i32, u128>(ctx, l, r, gt.span()))
            }
            // Int8
            (ColumnData::Int8(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<i64, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<i64, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<i64, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<i64, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<i64, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<i64, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<i64, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<i64, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<i64, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<i64, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<i64, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int8(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<i64, u128>(ctx, l, r, gt.span()))
            }
            // Int16
            (ColumnData::Int16(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<i128, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<i128, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<i128, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<i128, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<i128, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<i128, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<i128, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<i128, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<i128, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<i128, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<i128, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Int16(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<i128, u128>(ctx, l, r, gt.span()))
            }
            // Uint1
            (ColumnData::Uint1(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<u8, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<u8, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<u8, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<u8, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<u8, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<u8, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<u8, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<u8, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<u8, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<u8, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<u8, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<u8, u128>(ctx, l, r, gt.span()))
            }
            // Uint2
            (ColumnData::Uint2(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<u16, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<u16, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<u16, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<u16, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<u16, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<u16, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<u16, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<u16, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<u16, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<u16, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<u16, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<u16, u128>(ctx, l, r, gt.span()))
            }
            // Uint4
            (ColumnData::Uint4(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<u32, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<u32, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<u32, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<u32, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<u32, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<u32, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<u32, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<u32, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<u32, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<u32, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<u32, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<u32, u128>(ctx, l, r, gt.span()))
            }
            // Uint8
            (ColumnData::Uint8(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<u64, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<u64, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<u64, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<u64, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<u64, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<u64, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<u64, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<u64, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<u64, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<u64, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<u64, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<u64, u128>(ctx, l, r, gt.span()))
            }
            // Uint16
            (ColumnData::Uint16(l), ColumnData::Float4(r)) => {
                Ok(compare_number::<u128, f32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Float8(r)) => {
                Ok(compare_number::<u128, f64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Int1(r)) => {
                Ok(compare_number::<u128, i8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Int2(r)) => {
                Ok(compare_number::<u128, i16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Int4(r)) => {
                Ok(compare_number::<u128, i32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Int8(r)) => {
                Ok(compare_number::<u128, i64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Int16(r)) => {
                Ok(compare_number::<u128, i128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
                Ok(compare_number::<u128, u8>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
                Ok(compare_number::<u128, u16>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
                Ok(compare_number::<u128, u32>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
                Ok(compare_number::<u128, u64>(ctx, l, r, gt.span()))
            }
            (ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
                Ok(compare_number::<u128, u128>(ctx, l, r, gt.span()))
            }
            (ColumnData::Date(l), ColumnData::Date(r)) => Ok(compare_temporal(l, r, gt.span())),
            (ColumnData::DateTime(l), ColumnData::DateTime(r)) => {
                Ok(compare_temporal(l, r, gt.span()))
            }
            (ColumnData::Time(l), ColumnData::Time(r)) => Ok(compare_temporal(l, r, gt.span())),
            (ColumnData::Interval(l), ColumnData::Interval(r)) => {
                Ok(compare_temporal(l, r, gt.span()))
            }
            (ColumnData::Utf8(l), ColumnData::Utf8(r)) => Ok(compare_utf8(l, r, gt.span())),
            (ColumnData::Undefined(container), _) | (_, ColumnData::Undefined(container)) => {
                let span = gt.span();
                Ok(Column::ColumnQualified(ColumnQualified {
                    name: span.fragment.into(),
                    data: ColumnData::bool(vec![false; container.len()]),
                }))
            }
            _ => return_error!(greater_than_cannot_be_applied_to_incompatible_types(
                gt.span(),
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
) -> Column
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
                data.push(value::number::is_greater_than(*l, *r));
            }
            _ => data.push_undefined(),
        }
    }

    Column::ColumnQualified(ColumnQualified { name: span.fragment.into(), data })
}

fn compare_temporal<T>(
    l: &TemporalContainer<T>,
    r: &TemporalContainer<T>,
    span: OwnedSpan,
) -> Column
where
    T: IsTemporal + Clone + Debug + Default,
{
    debug_assert_eq!(l.len(), r.len());

    let mut data = Vec::with_capacity(l.len());
    let mut bitvec = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                data.push(temporal::is_greater_than(*l, *r));
                bitvec.push(true);
            }
            _ => {
                data.push(false);
                bitvec.push(false);
            }
        }
    }

    Column::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        data: ColumnData::bool_with_bitvec(data, bitvec),
    })
}

fn compare_utf8(l: &StringContainer, r: &StringContainer, span: OwnedSpan) -> Column {
    debug_assert_eq!(l.len(), r.len());

    let mut data = Vec::with_capacity(l.len());
    let mut bitvec = Vec::with_capacity(l.len());

    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                data.push(*l > *r);
                bitvec.push(true);
            }
            _ => {
                data.push(false);
                bitvec.push(false);
            }
        }
    }

    Column::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        data: ColumnData::bool_with_bitvec(data, bitvec),
    })
}
