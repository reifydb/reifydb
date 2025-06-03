// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::Evaluator;
use reifydb_frame::ColumnValues;
use reifydb_rql::expression::ConstantExpression;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        expr: ConstantExpression,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match expr {
            ConstantExpression::Undefined => ColumnValues::Undefined(row_count),
            ConstantExpression::Bool(v) => ColumnValues::bool(vec![v; row_count]),
            ConstantExpression::Number(s) => {
                // FIXME that does not look right..
                // Try parsing in order from most specific to most general
                if let Ok(v) = s.parse::<i8>() {
                    ColumnValues::int1(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i16>() {
                    ColumnValues::int2(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i32>() {
                    ColumnValues::int4(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i64>() {
                    ColumnValues::int8(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i128>() {
                    ColumnValues::int16(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u8>() {
                    ColumnValues::uint1(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u16>() {
                    ColumnValues::uint2(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u32>() {
                    ColumnValues::uint4(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u64>() {
                    ColumnValues::uint8(vec![v; row_count])
                } else if let Ok(v) = s.parse::<f32>() {
                    ColumnValues::float4(vec![v; row_count])
                } else if let Ok(v) = s.parse::<f64>() {
                    ColumnValues::float8(vec![v; row_count])
                } else {
                    // return Err(evaluate::Error::InvalidConstantNumber(s));
                    unimplemented!()
                }
            }
            ConstantExpression::Text(s) => {
                ColumnValues::string(std::iter::repeat(s).take(row_count))
            }
        })
    }
}
