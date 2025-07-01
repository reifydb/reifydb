// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstFrom;
use crate::plan::logical::{Compiler, LogicalQueryPlan, TableScanNode};

impl Compiler {
    pub(crate) fn compile_from(&self, ast: AstFrom) -> crate::Result<LogicalQueryPlan> {
        match ast {
            AstFrom::Table { schema, table, .. } => {
                Ok(LogicalQueryPlan::TableScan(TableScanNode {
                    schema: schema.map(|schema| schema.span()),
                    table: table.span(),
                }))
            }
            AstFrom::Query { .. } => unimplemented!(),
        }
    }
}
