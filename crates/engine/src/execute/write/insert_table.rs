// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::execute::Executor;
use reifydb_core::Value;
use reifydb_rql::expression::{ConstantExpression, Expression, PrefixExpression, PrefixOperator};
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
                            Expression::Constant(const_expr) => {
                                row_values.push(const_expr.into_column_value(column)?)
                            }
                            Expression::Prefix(PrefixExpression { operator, expression }) => {
                                match operator {
                                    PrefixOperator::Minus => match *expression {
                                        Expression::Constant(const_expr) => {
                                            row_values.push(match const_expr {
                                                ConstantExpression::Undefined => Value::Undefined,
                                                ConstantExpression::Bool(_) => Value::Undefined,
                                                ConstantExpression::Number(n) => {
                                                    ConstantExpression::Number(format!("-{n}"))
                                                        .into_column_value(column)?
                                                }
                                                ConstantExpression::Text(_) => Value::Undefined,
                                            })
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
