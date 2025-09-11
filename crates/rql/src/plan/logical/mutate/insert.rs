// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstInsert,
	plan::logical::{
		Compiler, InsertNode, LogicalPlan, resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_insert<'a, 't, T: CatalogQueryTransaction>(
		ast: AstInsert<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Convert MaybeQualified to fully qualified using resolver
		let target = resolver.resolve_maybe_source(&ast.target)?;

		Ok(LogicalPlan::Insert(InsertNode {
			target,
		}))
	}
}
