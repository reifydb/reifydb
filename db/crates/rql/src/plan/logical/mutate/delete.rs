// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstDelete,
	plan::logical::{
		Compiler, DeleteNode, LogicalPlan, resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_delete<'a, 't, T: CatalogQueryTransaction>(
		ast: AstDelete<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Resolve directly to TableIdentifier since DELETE only works
		// on tables
		let target = if let Some(t) = &ast.target {
			Some(resolver.resolve_maybe_qualified_table(t, true)?)
		} else {
			None
		};

		Ok(LogicalPlan::Delete(DeleteNode {
			target,
			input: None,
		}))
	}
}
