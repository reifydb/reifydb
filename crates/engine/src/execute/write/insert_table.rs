// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::evaluate::{Context, EvaluationColumn, evaluate};
use crate::execute::Executor;
use reifydb_core::CowVec;
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
                                        dbg!(&cvs);
                                        let r = match cvs {
                                            ColumnValues::Int2(values, validity) => {
                                                let slice = values.as_slice();
                                                // let v = demote_i16_to_i8_checked(slice).unwrap();
                                                let mut out = Vec::with_capacity(slice.len());

                                                for (i, &val) in slice.iter().enumerate() {
                                                    out.push(context.demote(val, &span)?.unwrap());
                                                }

                                                ColumnValues::Int1(CowVec::new(out), validity)
                                            }
                                            _ => unimplemented!(),
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

fn demote_i16_to_i8_checked(input: &[i16]) -> Result<Vec<i8>, usize> {
    let mut out = Vec::with_capacity(input.len());
    for (i, &val) in input.iter().enumerate() {
        match i8::try_from(val) {
            Ok(v) => out.push(v),
            Err(err) => {
                dbg!(err);
            }
        }
    }
    Ok(out)
}
