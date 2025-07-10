// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;
use crate::evaluate::{Context, EvaluationColumn, evaluate};
use crate::execute::Executor;
use crate::frame::Frame;
use reifydb_catalog::Catalog;
use reifydb_catalog::key::{EncodableKey, TableRowKey};
use reifydb_catalog::sequence::TableRowSequence;
use reifydb_core::interface::{Tx, UnversionedStorage, VersionedStorage};
use reifydb_core::row::Layout;
use reifydb_core::{BitVec, DataType, Value};
use reifydb_diagnostic::catalog::table_not_found;
use reifydb_rql::plan::physical::InsertIntoTablePlan;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn insert_into_table(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: InsertIntoTablePlan,
    ) -> crate::Result<Frame> {
        match plan {
            InsertIntoTablePlan::Values { schema, table, columns, rows_to_insert } => {
                let mut rows = Vec::with_capacity(rows_to_insert.len());

                let values: Vec<DataType> = columns.iter().map(|c| c.value).collect();
                let layout = Layout::new(&values);

                for row_to_insert in rows_to_insert {
                    let mut row = layout.allocate_row();

                    for (idx, expr) in row_to_insert.into_iter().enumerate() {
                        let column = &columns[idx];

                        let context = Context {
                            column: Some(EvaluationColumn {
                                name: Some(column.name.clone()),
                                data_type: Some(column.value),
                                policies: column
                                    .policies
                                    .iter()
                                    .map(|cp| cp.policy.clone())
                                    .collect(),
                            }),
                            mask: BitVec::empty(),
                            columns: Vec::new(),
                            row_count: 1,
                            limit: None,
                        };

                        // let span = expr.span().clone();
                        let lazy_span = expr.lazy_span();
                        match &expr {
                            expr => {
                                let cvs = evaluate(expr, &context)?.values;
                                match cvs.len() {
                                    1 => {
                                        // FIXME ensure its the right value
                                        let r =
                                            cvs.adjust_column(column.value, &context, &lazy_span)?;

                                        match r.get(0) {
                                            Value::Bool(v) => layout.set_bool(&mut row, idx, v),
                                            Value::Float4(v) => layout.set_f32(&mut row, idx, *v),
                                            Value::Float8(v) => layout.set_f64(&mut row, idx, *v),
                                            Value::Int1(v) => layout.set_i8(&mut row, idx, v),
                                            Value::Int2(v) => layout.set_i16(&mut row, idx, v),
                                            Value::Int4(v) => layout.set_i32(&mut row, idx, v),
                                            Value::Int8(v) => layout.set_i64(&mut row, idx, v),
                                            Value::Int16(v) => layout.set_i128(&mut row, idx, v),
                                            Value::Utf8(v) => layout.set_str(&mut row, idx, v),
                                            Value::Uint1(v) => layout.set_u8(&mut row, idx, v),
                                            Value::Uint2(v) => layout.set_u16(&mut row, idx, v),
                                            Value::Uint4(v) => layout.set_u32(&mut row, idx, v),
                                            Value::Uint8(v) => layout.set_u64(&mut row, idx, v),
                                            Value::Uint16(v) => layout.set_u128(&mut row, idx, v),
                                            Value::Undefined => layout.set_undefined(&mut row, idx),
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
                let Some(table) = Catalog::get_table_by_name(tx, schema.id, &table.fragment)?
                else {
                    return Err(Error::execution(table_not_found(
                        table.clone(),
                        &schema.name,
                        &table.fragment,
                    )));
                };

                let inserted = rows.len();
                for row in rows {
                    let row_id = TableRowSequence::next_row_id(tx, table.id)?;
                    tx.set(&TableRowKey { table: table.id, row: row_id }.encode(), row).unwrap();
                }

                Ok(Frame::single_row([
                    ("schema", Value::Utf8(schema.name)),
                    ("table", Value::Utf8(table.name)),
                    ("inserted", Value::Uint8(inserted as u64)),
                ]))
            }
        }
    }
}
