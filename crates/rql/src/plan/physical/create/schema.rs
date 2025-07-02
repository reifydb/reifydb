// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::plan::logical::CreateSchemaNode;
use crate::plan::physical::{Compiler, CreateSchemaPlan, PhysicalPlan};
use reifydb_core::interface::Rx;

impl Compiler {
    pub(crate) fn compile_create_schema(
        _rx: &mut impl Rx,
        create: CreateSchemaNode,
    ) -> crate::Result<PhysicalPlan> {
        // FIXME validate catalog
        Ok(PhysicalPlan::CreateSchema(CreateSchemaPlan {
            schema: create.schema,
            if_not_exists: create.if_not_exists,
        }))
    }
}
