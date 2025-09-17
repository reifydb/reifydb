// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstUpdate,
	plan::logical::{Compiler, LogicalPlan, UpdateNode, resolver::IdentifierResolver},
};

impl Compiler {
	pub(crate) fn compile_update<'a, 't, T: CatalogQueryTransaction>(
		ast: AstUpdate<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Resolve the unresolved source to a table
		// (UPDATE currently only supports tables, not ring buffers)
		let target = if let Some(unresolved) = &ast.target {
			// Try to resolve as table
			Some(resolver.resolve_source_as_table(unresolved.namespace.as_ref(), &unresolved.name, true)?)
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
