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
