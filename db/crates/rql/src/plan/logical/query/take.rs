// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstTake,
	plan::logical::{
		Compiler, LogicalPlan, TakeNode, resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_take<'a, 't, T: CatalogQueryTransaction>(
		ast: AstTake<'a>,
		_resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Take(TakeNode {
			take: ast.take,
		}))
	}
}
