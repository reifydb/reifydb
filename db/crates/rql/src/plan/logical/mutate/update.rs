// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstUpdate,
	plan::logical::{
		Compiler, LogicalPlan, UpdateNode, resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_update<'a, 't, T: CatalogQueryTransaction>(
		ast: AstUpdate<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Resolve directly to TableIdentifier since UPDATE only works
		// on tables
		let target = if let Some(t) = &ast.target {
			Some(resolver.resolve_maybe_qualified_table(t, true)?)
		} else {
			None
		};

		Ok(LogicalPlan::Update(UpdateNode {
			target,
			input: None, /* Input will be set by the pipeline
			              * builder */
		}))
	}
}
