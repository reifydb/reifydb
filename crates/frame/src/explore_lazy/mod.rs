// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues, ValueRef};
use reifydb_core::{BitVec, Value};

#[derive(Debug)]
pub enum Expression {
    Column(String),
    Literal(Value),
    Add(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
}

pub fn evaluate_expression(expr: &Expression, columns: &[Column], mask: &BitVec) -> ColumnValues {
    match expr {
        Expression::Column(name) => {
            let col = columns.iter().find(|c| &c.name == name).expect("Unknown column");
            match &col.data.get(0) {
                ValueRef::Int2(_) => {
                    let mut values = Vec::new();
                    let mut valid = Vec::new();
                    for (i, v) in col.data.iter().enumerate() {
                        if mask.get(i) {
                            if let Value::Int2(n) = v {
                                values.push(n);
                                valid.push(true);
                            } else {
                                values.push(0);
                                valid.push(false);
                            }
                        }
                    }
                    ColumnValues::int2_with_validity(values, valid)
                }
                ValueRef::Bool(_) => {
                    let mut values = Vec::new();
                    let mut valid = Vec::new();
                    for (i, v) in col.data.iter().enumerate() {
                        if mask.get(i) {
                            if let Value::Bool(b) = v {
                                values.push(b);
                                valid.push(true);
                            } else {
                                values.push(false);
                                valid.push(false);
                            }
                        }
                    }
                    ColumnValues::bool_with_validity(values, valid)
                }
                _ => unimplemented!(),
            }
        }
        Expression::Literal(val) => match val {
            Value::Int2(n) => {
                ColumnValues::int2_with_validity(vec![*n; mask.len()], vec![true; mask.len()])
            }
            Value::Bool(b) => {
                ColumnValues::bool_with_validity(vec![*b; mask.len()], vec![true; mask.len()])
            }
            _ => unimplemented!(),
        },
        Expression::Add(lhs, rhs) => {
            match (evaluate_expression(lhs, columns, mask), evaluate_expression(rhs, columns, mask))
            {
                (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                    let mut values = Vec::new();
                    let mut valid = Vec::new();
                    for i in 0..lv.len() {
                        if lv_valid[i] && rv_valid[i] {
                            values.push(lv[i] + rv[i]);
                            valid.push(true);
                        } else {
                            values.push(0);
                            valid.push(false);
                        }
                    }
                    ColumnValues::int2_with_validity(values, valid)
                }
                _ => panic!("Add only supports Int2"),
            }
        }
        Expression::Multiply(lhs, rhs) => {
            match (evaluate_expression(lhs, columns, mask), evaluate_expression(rhs, columns, mask))
            {
                (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                    let mut values = Vec::new();
                    let mut valid = Vec::new();
                    for i in 0..lv.len() {
                        if lv_valid[i] && rv_valid[i] {
                            values.push(lv[i] * rv[i]);
                            valid.push(true);
                        } else {
                            values.push(0);
                            valid.push(false);
                        }
                    }
                    ColumnValues::int2_with_validity(values, valid)
                }
                _ => panic!("Multiply only supports Int2"),
            }
        }
        Expression::GreaterThan(lhs, rhs) => {
            match (evaluate_expression(lhs, columns, mask), evaluate_expression(rhs, columns, mask))
            {
                (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                    let mut values = Vec::new();
                    let mut valid = Vec::new();
                    for i in 0..lv.len() {
                        if lv_valid[i] && rv_valid[i] {
                            values.push(lv[i] > rv[i]);
                            valid.push(true);
                        } else {
                            values.push(false);
                            valid.push(false);
                        }
                    }
                    ColumnValues::bool_with_validity(values, valid)
                }
                _ => panic!("GT only supports Int2"),
            }
        }
    }
}

// pub struct LazyFrame {
//     pub columns: Vec<Column>,
//     pub expressions: Vec<(String, Expression)>,
//     pub filter: Option<Expression>,
//     pub row_count: usize,
// }
//
// impl LazyFrame {
//     pub fn evaluate(&self) -> Vec<(String, ColumnValues)> {
//         let mask = self.compute_mask();
//
//         self.expressions
//             .iter()
//             .map(|(alias, expr)| {
//                 let values = evaluate_expression(expr, &self.columns, &mask);
//                 (alias.clone(), values)
//             })
//             .collect()
//     }
//
//     fn compute_mask(&self) -> BitVec {
//         if let Some(filter_expr) = &self.filter {
//             let raw =
//                 evaluate_expression(filter_expr, &self.columns, &BitVec::new(self.row_count, true));
//             if let ColumnValues::Bool(values, valid) = raw {
//                 BitVec::from_fn(values.len(), |i| valid[i] && values[i])
//             } else {
//                 panic!("Filter must be boolean");
//             }
//         } else {
//             BitVec::new(self.row_count, true)
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use crate::ColumnValues;
    use crate::explore_lazy::{Column, Expression, Value};
    use crate::lazy::LazyFrame;
    use std::vec;

    #[test]
    fn test() {
        let col_price =
            Column { name: "price".to_string(), data: ColumnValues::int2(vec![10, 100, 200, 300]) };
        let col_fee =
            Column { name: "fee".to_string(), data: ColumnValues::int2(vec![4, 1, 2, 3]) };

        let frame = LazyFrame {
            columns: vec![col_price, col_fee],
            filter: vec![Expression::GreaterThan(
                Box::new(Expression::Column("price".to_string())),
                Box::new(Expression::Literal(Value::Int2(100))),
            )],
            expressions: vec![(
                "total".to_string(),
                Expression::Add(
                    Box::new(Expression::Column("price".to_string())),
                    Box::new(Expression::Column("fee".to_string())),
                ),
            )],
        };

        let result = frame.evaluate();

        for (name, values) in result {
            println!("{}: {:?}", name, values);
        }
    }
}
