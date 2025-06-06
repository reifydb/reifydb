// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::evaluate::{Context, EvaluationColumn, evaluate};
use crate::execute::Executor;
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

                        match expr {
                            expr => {
                                let cvs = evaluate(
                                    expr,
                                    &Context {
                                        column: Some(EvaluationColumn {
                                            name: column.name.clone(),
                                            value: column.value,
                                        }),
                                    },
                                    &[],
                                    1,
                                )?;
                                match cvs.len() {
                                    1 => {
                                        row_values.push(cvs.get(0).as_value());
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
