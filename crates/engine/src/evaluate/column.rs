// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Error, Evaluator};
use crate::frame::{Column, ColumnValues};
use reifydb_core::Value;
use reifydb_diagnostic::query::column_not_found;
use reifydb_rql::expression::ColumnExpression;

impl Evaluator {
    pub(crate) fn column(
        &mut self,
        column: &ColumnExpression,
        ctx: &Context,
    ) -> evaluate::Result<Column> {
        let name = column.0.fragment.to_string();
        let col = ctx
            .columns
            .iter()
            .find(|c| &c.name == name.as_str())
            .ok_or(Error(column_not_found(column.0.clone())))?;

        let limit = ctx.limit.unwrap_or(usize::MAX);

        match col.data.get(0) {
            Value::Bool(_) => {
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
                Ok(Column { name, data: ColumnValues::bool_with_validity(values, valid) })
            }

            Value::Float4(_) => {
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
                Ok(Column { name, data: ColumnValues::float4_with_validity(values, valid) })
            }

            Value::Float8(_) => {
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
                Ok(Column { name, data: ColumnValues::float8_with_validity(values, valid) })
            }

            Value::Int1(_) => {
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
                Ok(Column { name, data: ColumnValues::int1_with_validity(values, valid) })
            }

            Value::Int2(_) => {
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
                Ok(Column { name, data: ColumnValues::int2_with_validity(values, valid) })
            }

            Value::Int4(_) => {
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
                Ok(Column { name, data: ColumnValues::int4_with_validity(values, valid) })
            }

            Value::Int8(_) => {
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
                Ok(Column { name, data: ColumnValues::int8_with_validity(values, valid) })
            }

            Value::Int16(_) => {
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
                Ok(Column { name, data: ColumnValues::int16_with_validity(values, valid) })
            }

            Value::String(_) => {
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
                Ok(Column { name, data: ColumnValues::string_with_validity(values, valid) })
            }

            Value::Uint1(_) => {
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
                Ok(Column { name, data: ColumnValues::uint1_with_validity(values, valid) })
            }

            Value::Uint2(_) => {
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
                Ok(Column { name, data: ColumnValues::uint2_with_validity(values, valid) })
            }

            Value::Uint4(_) => {
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
                Ok(Column { name, data: ColumnValues::uint4_with_validity(values, valid) })
            }

            Value::Uint8(_) => {
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
                Ok(Column { name, data: ColumnValues::uint8_with_validity(values, valid) })
            }

            Value::Uint16(_) => {
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
                Ok(Column { name, data: ColumnValues::uint16_with_validity(values, valid) })
            }

            _ => unimplemented!(),
        }
    }
}
