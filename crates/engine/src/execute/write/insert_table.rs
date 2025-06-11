// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::evaluate::{Context, EvaluationColumn, evaluate};
use crate::execute::Executor;
use crate::execute::write::column::adjust_column;
use reifydb_core::ValueKind;
use reifydb_core::row::Layout;
use reifydb_frame::ValueRef;
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

                let values: Vec<ValueKind> = columns.iter().map(|c| c.value).collect();
                let layout = Layout::new(&values);

                for row_to_insert in rows_to_insert {
                    // let mut row_values = Vec::with_capacity(row.len());

                    let mut row = layout.allocate_row();

                    for (idx, expr) in row_to_insert.into_iter().enumerate() {
                        let column = &columns[idx];

                        let context = Context {
                            column: Some(EvaluationColumn {
                                name: column.name.clone(),
                                value: column.value,
                                policies: column.policies.clone(),
                            }),
                            frame: None,
                        };

                        // let span = expr.span().clone();
                        let lazy_span = expr.lazy_span();
                        match &expr {
                            expr => {
                                let cvs = evaluate(expr, &context, &[], 1)?;
                                match cvs.len() {
                                    1 => {
                                        // FIXME ensure its the right value
                                        let r = adjust_column(
                                            column.value,
                                            &cvs,
                                            &context,
                                            &lazy_span,
                                        )?;
                                        // row_values.push(r.get(0).as_value());
                                        match r.get(0) {
                                            ValueRef::Int1(v) => {
                                                layout.set_i8(row.make_mut(), idx, *v)
                                            }
                                            ValueRef::Int2(v) => {
                                                layout.set_i16(row.make_mut(), idx, *v)
                                            }
                                            _ => unimplemented!(),
                                        }
                                    }
                                    _ => unimplemented!(),
                                }
                            }
                        }
                    }
                    rows.push(row);
                }

                let result = tx.insert_into_table(schema.as_str(), table.as_str(), rows).unwrap();
                Ok(ExecutionResult::InsertIntoTable { schema, table, inserted: result.inserted })
            }
        }
    }
}
