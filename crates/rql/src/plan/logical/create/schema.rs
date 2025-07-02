// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstCreateSchema;
use crate::plan::logical::{Compiler, CreateSchemaNode, LogicalPlan};

impl Compiler {
    pub(crate) fn compile_create_schema(ast: AstCreateSchema) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::CreateSchema(CreateSchemaNode {
            schema: ast.name.span(),
            if_not_exists: false,
        }))
    }
}
