// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstFrom;
use crate::plan::logical::{Compiler, LogicalQueryPlan, TableScanNode};
use std::vec;

impl Compiler {
    pub(crate) fn compile_from(&self, ast: AstFrom) -> crate::Result<Vec<LogicalQueryPlan>> {
        match ast {
            AstFrom::Table { schema, table, .. } => {
                Ok(vec![LogicalQueryPlan::TableScan(TableScanNode {
                    schema: schema.map(|schema| schema.span()),
                    table: table.span(),
                })])
            }
            AstFrom::Query { .. } => unimplemented!(),
        }
    }
}
