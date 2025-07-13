// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::expression::{Expression, TupleExpression};
use crate::plan::logical::InsertIntoTableNode;
use crate::plan::physical::{Compiler, InsertIntoTablePlan, PhysicalPlan};
use reifydb_catalog::Catalog;
use reifydb_core::interface::Rx;
use reifydb_core::{Error, Span};
use reifydb_diagnostic::catalog::table_not_found;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;

impl Compiler {
    pub(crate) fn compile_insert_into_table(
        rx: &mut impl Rx,
        insert: InsertIntoTableNode,
    ) -> crate::Result<PhysicalPlan> {
        let InsertIntoTableNode::Values { schema, table, columns, rows_to_insert } = insert;

        let schema = schema.to_string();
        let store = table;

        let schema = Catalog::get_schema_by_name(rx, &schema).unwrap().unwrap();
        let Some(table) = Catalog::get_table_by_name(rx, schema.id, &store.fragment).unwrap()
        else {
            return Err(Error(table_not_found(store.clone(), &schema.name, &store.fragment)));
        };

        // Build the user-specified column name list
        let insert_column_names: Vec<_> =
            columns.into_iter().map(|column| column.to_string()).collect::<Vec<_>>();

        // Lookup actual columns from the store
        let columns_to_insert: Vec<_> = insert_column_names
            .iter()
            .map(|name| Catalog::get_column_by_name(rx, table.id, name.deref()).unwrap().unwrap())
            .collect::<Vec<_>>();

        // Create a mapping: column name -> position in insert input
        let insert_index_map: HashMap<_, _> =
            insert_column_names.iter().enumerate().map(|(i, name)| (name.to_string(), i)).collect();

        // Now reorder the row expressions to match store_schema.column order
        let rows_to_insert = rows_to_insert
            .into_iter()
            .map(|mut row| {
                let mut values = vec![None; columns_to_insert.len()];

                for (col_idx, col) in table.columns.iter().enumerate() {
                    if let Some(&input_idx) = insert_index_map.get(&col.name) {
                        let expr = mem::replace(
                            &mut row[input_idx],
                            Expression::Tuple(TupleExpression {
                                expressions: vec![],
                                span: Span::testing(""),
                            }),
                        );

                        values[col_idx] = Some(expr);
                    } else {
                        // Not provided in INSERT, use default
                        unimplemented!()
                    }
                }

                values.into_iter().map(|v| v.unwrap()).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let columns = table.columns;

        Ok(PhysicalPlan::InsertIntoTable(InsertIntoTablePlan::Values {
            schema: Span::testing(schema.name),
            // FIXME
            table: store,
            columns,
            rows_to_insert,
        }))
    }
}
