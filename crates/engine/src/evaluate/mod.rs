// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Value;
use base::Value::Undefined;
use base::expression::Expression;
use dataframe::{Column, ColumnValues};

pub fn evaluate<'a>(
    expr: &Expression,
    columns: &[&'a Column],
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
            let left = evaluate(left, columns, row_count)?;
            let right = evaluate(right, columns, row_count)?;

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

        Expression::Call(call) => {
            let mut args = vec![];
            for arg in &call.args {
                match arg {
                    Expression::Column(col_name) => {
                        args.push(
                            columns.iter().find(|c| c.name == *col_name).map(|c| *c).unwrap(),
                        );
                    }
                    _ => return Err("only column arguments supported for now".into()),
                }
            }

            Ok(avg(&args, row_count))
        }

        _ => unimplemented!(),
    }
}

pub fn avg(arg_columns: &[&Column], row_count: usize) -> ColumnValues {
    let mut values = Vec::with_capacity(row_count);
    let mut valids = Vec::with_capacity(row_count);

    for row in 0..row_count {
        let mut sum = 0f64;
        let mut count = 0;

        for col in arg_columns {
            match &col.data {
                ColumnValues::Int2(vals, validity) => {
                    if validity.get(row).copied().unwrap_or(false) {
                        sum += vals[row] as f64;
                        count += 1;
                    }
                }
                ColumnValues::Float8(vals, validity) => {
                    if validity.get(row).copied().unwrap_or(false) {
                        sum += vals[row];
                        count += 1;
                    }
                }
                _ => {
                    unimplemented!()
                }
            }
        }

        if count > 0 {
            values.push(sum / count as f64);
            valids.push(true);
        } else {
            values.push(0.0);
            valids.push(false);
        }
    }

    ColumnValues::float8_with_validity(values, valids)
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
