// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::{AstCreateIndex, AstIndexColumn},
	bump::BumpBox,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, CreateIndexNode, IndexColumn, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_index(&self, ast: AstCreateIndex<'bump>) -> Result<LogicalPlan<'bump>> {
		let columns = ast
			.columns
			.into_iter()
			.map(|col: AstIndexColumn| IndexColumn {
				column: col.column.name,
				order: col.order,
			})
			.collect();

		let filter = ast
			.filters
			.into_iter()
			.map(|filter_ast| ExpressionCompiler::compile(BumpBox::into_inner(filter_ast)))
			.collect::<Result<Vec<_>>>()?;

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
