// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{AstCreateIndex, AstIndexColumn};
use crate::expression::ExpressionCompiler;
use crate::plan::logical::{Compiler, CreateIndexNode, IndexColumn, LogicalPlan};

impl Compiler {
    pub(crate) fn compile_create_index(ast: AstCreateIndex) -> crate::Result<LogicalPlan> {
        let columns = ast
            .columns
            .into_iter()
            .map(|col: AstIndexColumn| IndexColumn { column: col.column.span(), order: col.order })
            .collect();

        let filter = ast
            .filters
            .into_iter()
            .map(|filter_ast| ExpressionCompiler::compile(*filter_ast))
            .collect::<Result<Vec<_>, _>>()?;

        let map = if let Some(map_ast) = ast.map {
            Some(ExpressionCompiler::compile(*map_ast)?)
        } else {
            None
        };

        Ok(LogicalPlan::CreateIndex(CreateIndexNode {
            index_type: ast.index_type,
            name: ast.name.span(),
            schema: ast.schema.span(),
            table: ast.table.span(),
            columns,
            filter,
            map,
        }))
    }
}
