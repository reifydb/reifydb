// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use crate::evaluate::{Error, EvaluationContext, Evaluator};
use crate::frame::{ColumnValues, FrameColumn};
use reifydb_core::Value;
use reifydb_core::diagnostic::query::column_not_found;
use reifydb_rql::expression::ColumnExpression;

impl Evaluator {
    pub(crate) fn column(
        &mut self,
        column: &ColumnExpression,
        ctx: &EvaluationContext,
    ) -> evaluate::Result<FrameColumn> {
        let name = column.0.fragment.to_string();
        let col = ctx
            .columns
            .iter()
            .find(|c| &c.name == name.as_str())
            .ok_or(Error(column_not_found(column.0.clone())))?;

        let take = ctx.take.unwrap_or(usize::MAX);

        match col.values.get(0) {
            Value::Bool(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Bool(b) => {
                                values.push(b);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(false);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::bool_with_bitvec(values, bitvec) })
            }

            Value::Float4(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Float4(v) => {
                                values.push(v.value());
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0.0f32);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::float4_with_bitvec(values, bitvec) })
            }

            Value::Float8(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Float8(v) => {
                                values.push(v.value());
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0.0f64);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::float8_with_bitvec(values, bitvec) })
            }

            Value::Int1(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Int1(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::int1_with_bitvec(values, bitvec) })
            }

            Value::Int2(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Int2(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::int2_with_bitvec(values, bitvec) })
            }

            Value::Int4(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Int4(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::int4_with_bitvec(values, bitvec) })
            }

            Value::Int8(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Int8(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::int8_with_bitvec(values, bitvec) })
            }

            Value::Int16(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Int16(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::int16_with_bitvec(values, bitvec) })
            }

            Value::Utf8(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Utf8(s) => {
                                values.push(s.clone());
                                bitvec.push(true);
                            }
                            _ => {
                                values.push("".to_string());
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::utf8_with_bitvec(values, bitvec) })
            }

            Value::Uint1(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Uint1(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::uint1_with_bitvec(values, bitvec) })
            }

            Value::Uint2(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Uint2(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::uint2_with_bitvec(values, bitvec) })
            }

            Value::Uint4(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Uint4(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::uint4_with_bitvec(values, bitvec) })
            }

            Value::Uint8(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Uint8(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::uint8_with_bitvec(values, bitvec) })
            }

            Value::Uint16(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for (i, v) in col.values.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= take {
                            break;
                        }
                        match v {
                            Value::Uint16(n) => {
                                values.push(n);
                                bitvec.push(true);
                            }
                            _ => {
                                values.push(0);
                                bitvec.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(FrameColumn { name, values: ColumnValues::uint16_with_bitvec(values, bitvec) })
            }

            _ => unimplemented!(),
        }
    }
}
