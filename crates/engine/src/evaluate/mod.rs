// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::function::{Function, math};
use base::Value;
use base::Value::Undefined;
use base::expression::Expression;
use dataframe::{Column, ColumnValues};

pub fn evaluate(
    expr: Expression,
    columns: &[&Column],
    row_count: usize,
) -> dataframe::Result<ColumnValues> {
    match expr {
        // FIXME this might be very expensive
        Expression::Column(name) => columns
            .iter()
            .find(|c| c.name == *name)
            .cloned()
            .cloned()
            .map(|c| c.data)
            .ok_or_else(|| format!("unknown column '{}'", name).into()),

        Expression::Add(left, right) => {
            let left = evaluate(*left, columns, row_count)?;
            let right = evaluate(*right, columns, row_count)?;

            match (&left, &right) {
                (ColumnValues::Int2(a_vals, a_valid), ColumnValues::Int2(b_vals, b_valid)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut valid = Vec::with_capacity(row_count);
                    for i in 0..row_count {
                        if a_valid[i] && b_valid[i] {
                            values.push(a_vals[i] + b_vals[i]);
                            valid.push(true);
                        } else {
                            values.push(0); // Placeholder
                            valid.push(false);
                        }
                    }
                    Ok(ColumnValues::Int2(values, valid))
                }
                _ => Ok(ColumnValues::Undefined(row_count)),
            }
        }

        Expression::Constant(v) => Ok(match v {
            Value::Bool(v) => ColumnValues::Bool(vec![v.clone(); row_count], vec![true; row_count]),
            Value::Float4(v) => unimplemented!(),
            Value::Float8(v) => {
                ColumnValues::Float8(vec![v.value(); row_count], vec![true; row_count])
            }
            Value::Int2(v) => ColumnValues::Int2(vec![v.clone(); row_count], vec![true; row_count]),
            Value::Text(v) => ColumnValues::Text(vec![v.clone(); row_count], vec![true; row_count]),
            Value::Uint2(v) => unimplemented!(),
            Undefined => ColumnValues::Undefined(row_count),
        }),

        // Expression::Column(column) => {
        //     Ok(columns.iter().find(|c| c.name == *column).cloned().cloned().unwrap().data)
        // }
        // Expression::Constant(v) => match v {
        //     // Value::Int2(v) => result.push(Column {
        //     //     name: "constant".to_string(),
        //     //     data: ColumnValues::Int2(vec![v; row_count], vec![true; row_count]),
        //     // }),
        //     Value::Int2(v) => Ok(ColumnValues::Int2(vec![v; row_count], vec![true; row_count])),
        //     _ => unimplemented!(),
        // },

        Expression::Call(call) => {
            let virtual_columns = evaluate_virtual_column(call.args, &columns, row_count).unwrap();

            let functor = math::AvgFunction {};
            let exec = functor.prepare().unwrap();

            Ok(exec.eval_scalar(&virtual_columns, row_count).unwrap())
        }

        _ => unimplemented!(),
    }
}

fn evaluate_virtual_column<'a>(
    expressions: Vec<Expression>,
    columns: &[&Column],
    row_count: usize,
) -> crate::Result<Vec<Column>> {
    let mut result: Vec<Column> = Vec::with_capacity(expressions.len());

    for expression in expressions {
        result.push(Column {
            name: expression.to_string(),
            data: evaluate(expression, columns, row_count)?,
        })
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
