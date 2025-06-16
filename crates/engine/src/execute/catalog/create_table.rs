// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_rql::plan::CreateTablePlan;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn create_table(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: CreateTablePlan,
    ) -> crate::Result<ExecutionResult> {
        // let Some(schema) = self.get_schema_by_name(tx, &plan.schema)? else {
        //     return Err(Error::execution(Diagnostic::schema_not_found(
        //         Some(plan.span),
        //         &plan.schema,
        //     )));
        // };
        //
        // if let Some(table) = self.get_table_by_name(tx, &plan.table)? {
        //     if plan.if_not_exists {
        //         return Ok(ExecutionResult::CreateTable(CreateTableResult {
        //             id: table.id,
        //             schema: plan.schema,
        //             table: plan.table,
        //             created: false,
        //         }));
        //     }
        //
        //     return Err(Error::execution(Diagnostic::table_already_exists(
        //         Some(plan.span),
        //         &schema.name,
        //         &table.name,
        //     )));
        // }
        //
        // let table_id = self.next_table_id(tx)?;
        // Self::store_table(tx, table_id, schema.id, &plan)?;
        // Self::link_table_to_schema(tx, table_id, schema.id)?;
        //
        // let schema = plan.schema.clone();
        // let table = plan.table.clone();
        //
        // self.insert_columns(tx, table_id, plan)?;
        //
        // Ok(ExecutionResult::CreateTable(CreateTableResult {
        //     id: table_id,
        //     schema,
        //     table,
        //     created: true,
        // }))
        todo!()
    }

    // fn store_table(
    //     tx: &mut impl Tx<VS, US>,
    //     table: TableId,
    //     schema: SchemaId,
    //     plan: &CreateTablePlan,
    // ) -> crate::Result<()> {
    //     let mut row = table::LAYOUT.allocate_row();
    //     table::LAYOUT.set_u32(&mut row, table::ID, table);
    //     table::LAYOUT.set_u32(&mut row, table::SCHEMA, schema);
    //     table::LAYOUT.set_str(&mut row, table::NAME, &plan.table);
    //
    //     tx.set(&Key::Table(TableKey { table }).encode(), row)?;
    //
    //     Ok(())
    // }
    //
    // fn link_table_to_schema(
    //     tx: &mut impl Tx<VS, US>,
    //     table: TableId,
    //     schema: SchemaId,
    // ) -> crate::Result<()> {
    //     let mut row = table_schema::LAYOUT.allocate_row();
    //     table_schema::LAYOUT.set_u32(&mut row, table_schema::ID, table);
    //     tx.set(&Key::SchemaTable(SchemaTableKey { schema, table }).encode(), row)?;
    //     Ok(())
    // }
    //
    // fn insert_columns(
    //     &mut self,
    //     tx: &mut impl Tx<VS, US>,
    //     table: TableId,
    //     plan: CreateTablePlan,
    // ) -> crate::Result<()> {
    //     for column_to_create in plan.columns {
    //         self.create_column(
    //             tx,
    //             table,
    //             ColumnToCreate {
    //                 span: None,
    //                 schema_name: &plan.schema,
    //                 table,
    //                 table_name: &plan.table,
    //                 column: column_to_create.name,
    //                 value: column_to_create.value,
    //                 if_not_exists: false,
    //                 policies: column_to_create.policies.clone(),
    //             },
    //         )?;
    //     }
    //     Ok(())
    // }
}
