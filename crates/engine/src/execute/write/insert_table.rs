// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::evaluate::{Context, EvaluationColumn, evaluate};
use crate::execute::Executor;
use reifydb_core::ValueKind;
use reifydb_frame::ColumnValues;
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

                for row in rows_to_insert {
                    let mut row_values = Vec::with_capacity(row.len());
                    for (idx, expr) in row.into_iter().enumerate() {
                        let column = &columns[idx];

                        let context = Context {
                            column: Some(EvaluationColumn {
                                name: column.name.clone(),
                                value: column.value,
                                policies: column.policies.clone(),
                            }),
                            frame: None,
                        };

                        let span = expr.span().clone();
                        match expr {
                            expr => {
                                let cvs = evaluate(expr, &context, &[], 1)?;
                                match cvs.len() {
                                    1 => {
                                        // FIXME ensure its the right value
                                        // otherwise try to demote
                                        // otherwise saturate according to the policy
                                        let r = match (column.value, &cvs) {
                                            (ValueKind::Int1, ColumnValues::Int1(_, _)) => cvs,
                                            (
                                                ValueKind::Int1,
                                                ColumnValues::Int2(values, validity),
                                            ) => {
                                                let slice = values.as_slice();
                                                let mut res = ColumnValues::with_capacity(
                                                    ValueKind::Int1,
                                                    slice.len(),
                                                );

                                                for (i, &val) in slice.iter().enumerate() {
                                                    if validity[i] {
                                                        match context.demote(val, &span)? {
                                                            Some(value) => {
                                                                res.push_i8(value);
                                                            }
                                                            None => res.push_undefined(),
                                                        }
                                                    } else {
                                                        res.push_undefined()
                                                    }
                                                }

                                                res
                                            }

                                            (_, _) => unimplemented!(),
                                        };

                                        row_values.push(r.get(0).as_value());
                                    }
                                    _ => unimplemented!(),
                                }
                            }
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
