// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::identifier::{
	ColumnIdentifier, ColumnSource, IndexIdentifier,
};
use reifydb_type::Fragment;

use crate::{
	ast::{AstCreateIndex, AstIndexColumn},
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, CreateIndexNode, IndexColumn, LogicalPlan,
		resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_create_index<
		'a,
		't,
		T: CatalogQueryTransaction,
	>(
		ast: AstCreateIndex<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Get the schema with default from resolver
		let schema = ast.index.schema.clone().unwrap_or_else(|| {
			Fragment::borrowed_internal(resolver.default_schema())
		});

		// Create the table source for column qualification
		let table_source = ColumnSource::Source {
			schema: schema.clone(),
			source: ast.index.table.clone(),
		};

		let columns = ast
			.columns
			.into_iter()
			.map(|col: AstIndexColumn| IndexColumn {
				column: ColumnIdentifier {
					source: table_source.clone(),
					name: col.column.name,
				},
				order: col.order,
			})
			.collect();

		let filter = ast
			.filters
			.into_iter()
			.map(|filter_ast| {
				ExpressionCompiler::compile(*filter_ast)
			})
			.collect::<Result<Vec<_>, _>>()?;

		let map = if let Some(map_ast) = ast.map {
			Some(ExpressionCompiler::compile(*map_ast)?)
		} else {
			None
		};

		let index = IndexIdentifier::new(
			schema,
			ast.index.table,
			ast.index.name,
		);

		Ok(LogicalPlan::CreateIndex(CreateIndexNode {
			index_type: ast.index_type,
			index,
			columns,
			filter,
			map,
		}))
	}
}
