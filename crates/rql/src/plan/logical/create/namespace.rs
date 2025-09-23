// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreateNamespace,
	plan::logical::{Compiler, CreateNamespaceNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_namespace<'a, T: CatalogQueryTransaction>(
		ast: AstCreateNamespace<'a>,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		// Use Fragment directly instead of NamespaceIdentifier
		let namespace = ast.namespace.name;

		Ok(LogicalPlan::CreateNamespace(CreateNamespaceNode {
			namespace,
			if_not_exists: false,
		}))
	}
}
