// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::expression::evaluate;
use crate::{Column, ColumnValues, DataFrame};
use base::Value;
use base::Value::Undefined;
use base::expression::AliasExpression;
use std::collections::HashMap;

impl DataFrame {
    pub fn project(&mut self, expressions: Vec<(AliasExpression)>) -> crate::Result<()> {
        let row_count = self.columns.first().map_or(0, |col| col.data.len());

        let col_map: HashMap<&str, &ColumnValues> =
            self.columns.iter().map(|c| (c.name.as_str(), &c.data)).collect();

        let mut new_columns = Vec::with_capacity(expressions.len());

        for expression in expressions {
            let name = expression.alias;
            let expr = expression.expression;

            let mut values = Vec::with_capacity(row_count);
            let mut valid = Vec::with_capacity(row_count);

            for row_idx in 0..row_count {
                let value = evaluate(&expr, &col_map, row_idx)?;

                match value {
                    Undefined => {
                        values.push(Undefined);
                        valid.push(false);
                    }
                    value => {
                        values.push(value);
                        valid.push(true);
                    }
                }
            }

            let column = match values.get(0) {
                Some(Value::Int2(_)) => {
                    let v = values
                        .into_iter()
                        .map(|v| match v {
                            Value::Int2(i) => i,
                            _ => 0,
                        })
                        .collect();
                    ColumnValues::Int2(v, valid)
                }
                Some(Value::Text(_)) => {
                    let v = values
                        .into_iter()
                        .map(|v| match v {
                            Value::Text(s) => s,
                            _ => "".to_string(),
                        })
                        .collect();
                    ColumnValues::Text(v, valid)
                }
                Some(Value::Bool(_)) => {
                    let v = values
                        .into_iter()
                        .map(|v| match v {
                            Value::Bool(b) => b,
                            _ => false,
                        })
                        .collect();
                    ColumnValues::Bool(v, valid)
                }
                _ => ColumnValues::Undefined(row_count),
            };

            new_columns.push(Column { name: name.unwrap_or("".to_string()), data: column });
        }

        self.columns = new_columns;
        Ok(())
    }
}
