// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_rql::plan::CreateSchemaPlan;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn create_schema(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: CreateSchemaPlan,
    ) -> crate::Result<ExecutionResult> {
        // if let Some(schema) = self.get_schema_by_name(tx, &plan.schema)? {
        //     if plan.if_not_exists {
        //         return Ok(ExecutionResult::CreateSchema(CreateSchemaResult {
        //             id: schema.id,
        //             schema: plan.schema,
        //             created: false,
        //         }));
        //     }
        //
        //     return Err(Error::execution(Diagnostic::schema_already_exists(
        //         Some(plan.span),
        //         &schema.name,
        //     )));
        // }
        //
        // let schema_id = self.next_schema_id(tx)?;
        //
        // let mut row = schema::LAYOUT.allocate_row();
        // schema::LAYOUT.set_u32(&mut row, schema::ID, schema_id);
        // schema::LAYOUT.set_str(&mut row, schema::NAME, &plan.schema);
        //
        // tx.set(&Key::Schema(SchemaKey { schema: schema_id }).encode(), row)?;
        //
        // Ok(ExecutionResult::CreateSchema(CreateSchemaResult {
        //     id: schema_id,
        //     schema: plan.schema,
        //     created: true,
        // }))
        todo!()
    }
}
