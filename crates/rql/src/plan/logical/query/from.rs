// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstFrom;
use crate::plan::logical::{Compiler, LogicalPlan, TableScanNode};

impl Compiler {
    pub(crate) fn compile_from(ast: AstFrom) -> crate::Result<LogicalPlan> {
        match ast {
            AstFrom::Table { schema, table, .. } => {
                Ok(LogicalPlan::TableScan(TableScanNode {
                    schema: schema.map(|schema| schema.span()),
                    table: table.span(),
                }))
            }
            AstFrom::Query { .. } => unimplemented!(),
        }
    }
}
