// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::{AstCreateIndex, AstIndexColumn},
	bump::BumpBox,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, CreateIndexNode, IndexColumn, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_index(&self, ast: AstCreateIndex<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		// Note: Column qualification will be handled during physical plan compilation

		let columns = ast
			.columns
			.into_iter()
			.map(|col: AstIndexColumn| IndexColumn {
				column: col.column.name, // Use just the name Fragment
				order: col.order,
			})
			.collect();

		let filter = ast
			.filters
			.into_iter()
			.map(|filter_ast| ExpressionCompiler::compile(BumpBox::into_inner(filter_ast)))
			.collect::<Result<Vec<_>, _>>()?;

		let map = if let Some(map_ast) = ast.map {
			Some(ExpressionCompiler::compile(BumpBox::into_inner(map_ast))?)
		} else {
			None
		};

		Ok(LogicalPlan::CreateIndex(CreateIndexNode {
			index_type: ast.index_type,
			index: ast.index,
			columns,
			filter,
			map,
		}))
	}
}
