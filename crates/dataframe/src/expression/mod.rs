// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ColumnValues;
use base::Value;
use base::Value::Undefined;
use base::expression::Expression;
use std::collections::HashMap;

pub(crate) fn evaluate<'a>(
    expression: &Expression,
    columns: &HashMap<&'a str, &'a ColumnValues>,
    row: usize,
) -> crate::Result<Value> {
    match expression {
        Expression::Column(name) => match columns.get(name.as_str()) {
            Some(ColumnValues::Int2(vals, valid)) => {
                Ok(if valid[row] { Value::Int2(vals[row]) } else { Undefined })
            }
            Some(ColumnValues::Text(vals, valid)) => {
                Ok(if valid[row] { Value::Text(vals[row].clone()) } else { Undefined })
            }
            Some(ColumnValues::Bool(vals, valid)) => {
                Ok(if valid[row] { Value::Bool(vals[row]) } else { Undefined })
            }
            Some(ColumnValues::Undefined(_)) => Ok(Undefined),
            None => Err(format!("unknown column '{}'", name).into()),
        },

        Expression::Add(l, lr) => {
            let l = evaluate(l, columns, row)?;
            let r = evaluate(lr, columns, row)?;
            match (l, r) {
                (Value::Int2(a), Value::Int2(b)) => Ok(Value::Int2(a + b)),
                _ => Ok(Undefined),
            }
        }
        Expression::And(_, _) => unimplemented!(),
        Expression::Or(_, _) => unimplemented!(),
        Expression::Not(_) => unimplemented!(),
        Expression::Constant(v) => Ok(v.clone()),
        Expression::Call(_) => unimplemented!(),
        Expression::Tuple(_) => unimplemented!(),
        Expression::Prefix(_) => unimplemented!(),
    }
}
