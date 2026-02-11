// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstCreateNamespace,
	plan::logical::{Compiler, CreateNamespaceNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_namespace(
		&self,
		ast: AstCreateNamespace<'bump>,
	) -> crate::Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateNamespace(CreateNamespaceNode {
			segments: ast.namespace.segments,
			if_not_exists: ast.if_not_exists,
		}))
	}
}
