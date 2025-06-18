// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Error, ExecutionResult};
use crate::evaluate::{Context, EvaluationColumn, evaluate};
use crate::execute::Executor;
use crate::execute::write::column::adjust_column;
use reifydb_catalog::Catalog;
use reifydb_catalog::key::{EncodableKey, TableRowKey};
use reifydb_catalog::sequence::TableRowSequence;
use reifydb_core::ValueKind;
use reifydb_core::row::Layout;
use reifydb_diagnostic::Diagnostic;
use crate::frame::ValueRef;
use reifydb_rql::plan::InsertIntoTablePlan;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn insert_into_table(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: InsertIntoTablePlan,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            InsertIntoTablePlan::Values { schema, table, columns, rows_to_insert } => {
                let mut rows = Vec::with_capacity(rows_to_insert.len());

                let values: Vec<ValueKind> = columns.iter().map(|c| c.value).collect();
                let layout = Layout::new(&values);

                for row_to_insert in rows_to_insert {
                    let mut row = layout.allocate_row();

                    for (idx, expr) in row_to_insert.into_iter().enumerate() {
                        let column = &columns[idx];

                        let context = Context {
                            column: Some(EvaluationColumn {
                                name: column.name.clone(),
                                value: column.value,
                                policies: column
                                    .policies
                                    .iter()
                                    .map(|cp| cp.policy.clone())
                                    .collect(),
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

                                        match r.get(0) {
                                            ValueRef::Bool(v) => layout.set_bool(&mut row, idx, *v),
                                            ValueRef::Float4(v) => {
                                                layout.set_f32(&mut row, idx, *v)
                                            }
                                            ValueRef::Float8(v) => {
                                                layout.set_f64(&mut row, idx, *v)
                                            }
                                            ValueRef::Int1(v) => layout.set_i8(&mut row, idx, *v),
                                            ValueRef::Int2(v) => layout.set_i16(&mut row, idx, *v),
                                            ValueRef::Int4(v) => layout.set_i32(&mut row, idx, *v),
                                            ValueRef::Int8(v) => layout.set_i64(&mut row, idx, *v),
                                            ValueRef::Int16(v) => {
                                                layout.set_i128(&mut row, idx, *v)
                                            }
                                            ValueRef::String(v) => layout.set_str(&mut row, idx, v),
                                            ValueRef::Uint1(v) => layout.set_u8(&mut row, idx, *v),
                                            ValueRef::Uint2(v) => layout.set_u16(&mut row, idx, *v),
                                            ValueRef::Uint4(v) => layout.set_u32(&mut row, idx, *v),
                                            ValueRef::Uint8(v) => layout.set_u64(&mut row, idx, *v),
                                            ValueRef::Uint16(v) => {
                                                layout.set_u128(&mut row, idx, *v)
                                            }
                                            ValueRef::Undefined => {
                                                layout.set_undefined(&mut row, idx)
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

                let schema = Catalog::get_schema_by_name(tx, &schema)?.unwrap();
                let Some(table) = Catalog::get_table_by_name(tx, schema.id, &table.fragment)?else{
                    return Err(Error::execution(Diagnostic::table_not_found(table.clone(), &schema.name, &table.fragment)))
                };

                // let table = TableId(1);

                let inserted = rows.len();
                for row in rows {
                    let row_id = TableRowSequence::next_row_id(tx, table.id)?;
                    tx.set(&TableRowKey { table: table.id, row: row_id }.encode(), row).unwrap();
                }

                // let result = tx.insert_into_table(table, rows).unwrap();
                Ok(ExecutionResult::InsertIntoTable {
                    schema: schema.name,
                    table: table.name,
                    inserted,
                })
            }
        }
    }
}
