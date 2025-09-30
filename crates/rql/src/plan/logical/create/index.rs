// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::{AstCreateIndex, AstIndexColumn},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, CreateIndexNode, IndexColumn, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_index<'a, T: CatalogQueryTransaction>(
		ast: AstCreateIndex<'a>,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
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
			.map(|filter_ast| ExpressionCompiler::compile(*filter_ast))
			.collect::<Result<Vec<_>, _>>()?;

		let map = if let Some(map_ast) = ast.map {
			Some(ExpressionCompiler::compile(*map_ast)?)
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
