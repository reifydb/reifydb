// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstFilter,
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, FilterNode, LogicalPlan, resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_filter<'a, 't, T: CatalogQueryTransaction>(
		ast: AstFilter<'a>,
		_resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Filter(FilterNode {
			condition: ExpressionCompiler::compile(*ast.node)?,
		}))
	}
}
