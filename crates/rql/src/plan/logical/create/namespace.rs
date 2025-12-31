// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::AstCreateNamespace,
	plan::logical::{Compiler, CreateNamespaceNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_namespace(&self, ast: AstCreateNamespace) -> crate::Result<LogicalPlan> {
		// Use Fragment directly instead of NamespaceIdentifier
		let namespace = ast.namespace.name;

		Ok(LogicalPlan::CreateNamespace(CreateNamespaceNode {
			namespace,
			if_not_exists: ast.if_not_exists,
		}))
	}
}
