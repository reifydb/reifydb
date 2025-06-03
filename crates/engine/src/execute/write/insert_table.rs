// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::execute::Executor;
use reifydb_core::ordered_float::{OrderedF32, OrderedF64};
use reifydb_core::{Value, ValueKind};
use reifydb_rql::expression::{Expression, ExpressionConstant, ExpressionPrefix, PrefixOperator};
use reifydb_rql::plan::InsertIntoTablePlan;
use reifydb_transaction::Tx;

impl Executor {
    pub(crate) fn insert_into_table(
        &mut self,
        tx: &mut impl Tx,
        plan: InsertIntoTablePlan,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            InsertIntoTablePlan::Values { schema, table, columns, rows_to_insert } => {
                let mut rows = Vec::with_capacity(rows_to_insert.len());

                // FIXME do not evaluate expression in here - you general evaluator and try to operate on columns
                for row in rows_to_insert {
                    let mut row_values = Vec::with_capacity(row.len());

                    for (idx, expr) in row.into_iter().enumerate() {
                        let column = &columns[idx];

                        match expr {
                            Expression::Constant(const_expr) => row_values.push(match const_expr {
                                ExpressionConstant::Bool(bool) => Value::Bool(bool),
                                ExpressionConstant::Number(number) => match column.value {
                                    ValueKind::Float4 => Value::Float4(
                                        OrderedF32::try_from(number.parse::<f32>().unwrap())
                                            .unwrap(),
                                    ),
                                    ValueKind::Float8 => Value::Float8(
                                        OrderedF64::try_from(number.parse::<f64>().unwrap())
                                            .unwrap(),
                                    ),
                                    ValueKind::Int1 => Value::Int1(number.parse::<i8>().unwrap()),
                                    ValueKind::Int2 => Value::Int2(number.parse::<i16>().unwrap()),
                                    ValueKind::Int4 => Value::Int4(number.parse::<i32>().unwrap()),
                                    ValueKind::Int8 => Value::Int8(number.parse::<i64>().unwrap()),
                                    ValueKind::Int16 => {
                                        Value::Int16(number.parse::<i128>().unwrap())
                                    }
                                    ValueKind::Uint1 => Value::Uint1(number.parse::<u8>().unwrap()),
                                    ValueKind::Uint2 => {
                                        Value::Uint2(number.parse::<u16>().unwrap())
                                    }
                                    ValueKind::Uint4 => {
                                        Value::Uint4(number.parse::<u32>().unwrap())
                                    }
                                    ValueKind::Uint8 => {
                                        Value::Uint8(number.parse::<u64>().unwrap())
                                    }
                                    ValueKind::Uint16 => {
                                        Value::Uint16(number.parse::<u128>().unwrap())
                                    }
                                    _ => unimplemented!(),
                                },
                                ExpressionConstant::Text(string) => Value::String(string),
                                ExpressionConstant::Undefined => Value::Undefined,
                            }),
                            Expression::Prefix(ExpressionPrefix { operator, expression }) => {
                                match operator {
                                    PrefixOperator::Minus => match *expression {
                                        Expression::Constant(const_expr) => {
                                            row_values.push(match const_expr {
                                                ExpressionConstant::Bool(bool) => Value::Bool(bool),
                                                ExpressionConstant::Number(number) => {
                                                    match column.value {
                                                        ValueKind::Float4 => Value::Float4(
                                                            OrderedF32::try_from(
                                                                -1.0f32
                                                                    * number
                                                                        .parse::<f32>()
                                                                        .unwrap(),
                                                            )
                                                            .unwrap(),
                                                        ),
                                                        ValueKind::Float8 => Value::Float8(
                                                            OrderedF64::try_from(
                                                                -1.0f64
                                                                    * number
                                                                        .parse::<f64>()
                                                                        .unwrap(),
                                                            )
                                                            .unwrap(),
                                                        ),
                                                        ValueKind::Int1 => Value::Int1(
                                                            -1 * number.parse::<i8>().unwrap(),
                                                        ),
                                                        ValueKind::Int2 => Value::Int2(
                                                            -1 * number.parse::<i16>().unwrap(),
                                                        ),
                                                        ValueKind::Int4 => Value::Int4(
                                                            -1 * number.parse::<i32>().unwrap(),
                                                        ),
                                                        ValueKind::Int8 => Value::Int8(
                                                            -1 * number.parse::<i64>().unwrap(),
                                                        ),
                                                        ValueKind::Int16 => Value::Int16(
                                                            -1 * number.parse::<i128>().unwrap(),
                                                        ),
                                                        _ => unimplemented!(),
                                                    }
                                                }
                                                ExpressionConstant::Text(string) => {
                                                    Value::String(string)
                                                }
                                                ExpressionConstant::Undefined => Value::Undefined,
                                            })

                                            // row_values.push(match const_expr {
                                            //     Value::Float4(v) => {
                                            //         Value::float4(-1.0f32 * v.value())
                                            //     }
                                            //     Value::Float8(v) => {
                                            //         Value::float8(-1.0f64 * v.value())
                                            //     }
                                            //     Value::Int1(v) => Value::Int1(-1 * v),
                                            //     Value::Int2(v) => Value::Int2(-1 * v),
                                            //     Value::Int4(v) => Value::Int4(-1 * v),
                                            //     Value::Int8(v) => Value::Int8(-1 * v),
                                            //     Value::Int16(v) => Value::Int16(-1 * v),
                                            //     _ => unreachable!(),
                                            // })
                                        }
                                        _ => unimplemented!(),
                                    },
                                    PrefixOperator::Plus => {}
                                }
                            }
                            expr => unimplemented!("{expr:?}"),
                        }
                    }
                    rows.push(row_values);
                }

                let result = tx.insert_into_table(schema.as_str(), table.as_str(), rows).unwrap();
                Ok(ExecutionResult::InsertIntoTable { schema, table, inserted: result.inserted })
            }
        }
    }
}
