// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Evaluator};
use crate::frame::{ColumnValues, ValueRef};
use reifydb_core::Value;
use reifydb_rql::expression::ColumnExpression;

impl Evaluator {
    pub(crate) fn column(
        &mut self,
        column: &ColumnExpression,
        ctx: &Context,
    ) -> evaluate::Result<ColumnValues> {
        let name = &column.0.fragment;
        let col = ctx.columns.iter().find(|c| &c.name == name).expect("Unknown column");

        let limit = ctx.limit.unwrap_or(usize::MAX);

        match col.data.get(0) {
            ValueRef::Bool(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Bool(b) => {
                                values.push(b);
                                valid.push(true);
                            }
                            _ => {
                                values.push(false);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::bool_with_validity(values, valid))
            }

            ValueRef::Float4(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Float4(v) => {
                                values.push(v.value());
                                valid.push(true);
                            }
                            _ => {
                                values.push(0.0f32);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            ValueRef::Float8(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Float8(v) => {
                                values.push(v.value());
                                valid.push(true);
                            }
                            _ => {
                                values.push(0.0f64);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::float8_with_validity(values, valid))
            }

            ValueRef::Int1(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Int1(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::int1_with_validity(values, valid))
            }

            ValueRef::Int2(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Int2(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::int2_with_validity(values, valid))
            }

            ValueRef::Int4(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Int4(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::int4_with_validity(values, valid))
            }

            ValueRef::Int8(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Int8(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::int8_with_validity(values, valid))
            }

            ValueRef::Int16(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Int16(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::int16_with_validity(values, valid))
            }

            ValueRef::String(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::String(s) => {
                                values.push(s.clone());
                                valid.push(true);
                            }
                            _ => {
                                values.push("".to_string());
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::string_with_validity(values, valid))
            }

            ValueRef::Uint1(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Uint1(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::uint1_with_validity(values, valid))
            }

            ValueRef::Uint2(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Uint2(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::uint2_with_validity(values, valid))
            }

            ValueRef::Uint4(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Uint4(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::uint4_with_validity(values, valid))
            }

            ValueRef::Uint8(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Uint8(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::uint8_with_validity(values, valid))
            }

            ValueRef::Uint16(_) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                let mut count = 0;
                for (i, v) in col.data.iter().enumerate() {
                    if ctx.mask.get(i) {
                        if count >= limit {
                            break;
                        }
                        match v {
                            Value::Uint16(n) => {
                                values.push(n);
                                valid.push(true);
                            }
                            _ => {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                        count += 1;
                    }
                }
                Ok(ColumnValues::uint16_with_validity(values, valid))
            }

            _ => unimplemented!(),
        }
    }
}
#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
