// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Value;
use base::Value::Undefined;
use base::expression::Expression;
use dataframe::ColumnValues;
use std::collections::HashMap;

pub mod function;

pub fn evaluate<'a>(
    expr: &Expression,
    columns: &HashMap<&'a str, &'a ColumnValues>,
    row_count: usize,
) -> dataframe::Result<ColumnValues> {
    match expr {
        // FIXME this might be very expensive
        Expression::Column(name) => columns
            .get(name.as_str())
            .cloned()
            .cloned()
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
                        args.push(columns[col_name.as_str()]);
                    }
                    _ => return Err("only column arguments supported for now".into()),
                }
            }

            let mut values = Vec::with_capacity(row_count);
            let mut valid = Vec::with_capacity(row_count);
            for row in 0..row_count {
                let val = avg_row(&args, row);
                match val {
                    Undefined => {
                        values.push(Undefined);
                        valid.push(false);
                    }
                    v => {
                        values.push(v);
                        valid.push(true);
                    }
                }
            }

            // Infer result type from `values[0]` if needed
            Ok(ColumnValues::from_values(values, valid))
        }

        _ => unimplemented!(),
    }
}

// pub(crate) fn evaluate<'a>(
//     expression: &Expression,
//     columns: &HashMap<&'a str, &'a ColumnValues>,
//     row: usize,
// ) -> crate::Result<Value> {
//     match expression {
//         Expression::Column(name) => match columns.get(name.as_str()) {
//             Some(ColumnValues::Float8(vals, valid)) => Ok(if valid[row] {
//                 Value::Float8(OrderedF64::try_from(vals[row]).unwrap())
//             } else {
//                 Undefined
//             }),
//             Some(ColumnValues::Int2(vals, valid)) => {
//                 Ok(if valid[row] { Value::Int2(vals[row]) } else { Undefined })
//             }
//             Some(ColumnValues::Text(vals, valid)) => {
//                 Ok(if valid[row] { Value::Text(vals[row].clone()) } else { Undefined })
//             }
//             Some(ColumnValues::Bool(vals, valid)) => {
//                 Ok(if valid[row] { Value::Bool(vals[row]) } else { Undefined })
//             }
//             Some(ColumnValues::Undefined(_)) => Ok(Undefined),
//             None => Err(format!("unknown column '{}'", name).into()),
//         },
//
//         Expression::Add(l, lr) => {
//             let l = evaluate(l, columns, row)?;
//             let r = evaluate(lr, columns, row)?;
//             match (l, r) {
//                 (Value::Int2(a), Value::Int2(b)) => Ok(Value::Int2(a + b)),
//                 _ => Ok(Undefined),
//             }
//         }
//         Expression::And(_, _) => unimplemented!(),
//         Expression::Or(_, _) => unimplemented!(),
//         Expression::Not(_) => unimplemented!(),
//         Expression::Constant(v) => Ok(v.clone()),
//         Expression::Call(call) => {
//             let mut arg_columns = vec![];
//
//             for a in &call.args {
//                 match a {
//                     Expression::Column(c) => {
//                         // args.push(c);
//                         arg_columns.push(columns[c.as_str()]);
//                     }
//                     _ => unimplemented!(),
//                 }
//             }
//
//             let r = avg_row(&arg_columns, row);
//             Ok(r)
//         }
//         Expression::Tuple(_) => unimplemented!(),
//         Expression::Prefix(_) => unimplemented!(),
//     }
// }

pub fn avg_row(arg_columns: &[&ColumnValues], row: usize) -> Value {
    let mut sum = 0f64;
    let mut count = 0;

    for col in arg_columns {
        match col {
            ColumnValues::Int2(values, valids) => {
                if valids.get(row).copied().unwrap_or(false) {
                    sum += values[row] as f64;
                    count += 1;
                }
            }
            // You can extend this to Float, Bool, etc. if needed
            _ => {}
        }
    }

    if count > 0 { Value::float8(sum / count as f64) } else { Value::Undefined }
}

// pub fn avg(arg_columns: &[ColumnValues]) -> ColumnValues {
//     let row_count = arg_columns.first().map_or(0, |col| col.len());
//
//     let mut result = Vec::with_capacity(row_count);
//     let mut valid = Vec::with_capacity(row_count);
//
//     for row_idx in 0..row_count {
//         let mut sum = 0i32;
//         let mut count = 0;
//
//         for col in arg_columns {
//             match col {
//                 ColumnValues::Int2(values, valids) => {
//                     if valids[row_idx] {
//                         sum += values[row_idx] as i32;
//                         count += 1;
//                     }
//                 }
//                 // optionally support Bool or other numeric types
//                 _ => {} // ignore non-int2
//             }
//         }
//
//         if count > 0 {
//             result.push((sum / count) as f64);
//             valid.push(true);
//         } else {
//             result.push(0.0); // placeholder
//             valid.push(false);
//         }
//     }
//
//     ColumnValues::Float8(result, valid)
// }

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
