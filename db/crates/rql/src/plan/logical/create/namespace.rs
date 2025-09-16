// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreateNamespace,
	plan::logical::{Compiler, CreateNamespaceNode, LogicalPlan, resolver::IdentifierResolver},
};

impl Compiler {
	pub(crate) fn compile_create_namespace<'a, 't, T: CatalogQueryTransaction>(
		ast: AstCreateNamespace<'a>,
		_resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Convert MaybeQualified to fully qualified
		use reifydb_core::interface::identifier::NamespaceIdentifier;

		let namespace = NamespaceIdentifier::new(ast.namespace.name);

		Ok(LogicalPlan::CreateNamespace(CreateNamespaceNode {
			namespace,
			if_not_exists: false,
		}))
	}
}
