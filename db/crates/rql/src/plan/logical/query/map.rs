// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstMap,
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, LogicalPlan, MapNode, resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_map<'a, 't, T: CatalogQueryTransaction>(
		ast: AstMap<'a>,
		_resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Map(MapNode {
			map: ast.nodes
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<crate::Result<Vec<_>>>()?,
		}))
	}
}
