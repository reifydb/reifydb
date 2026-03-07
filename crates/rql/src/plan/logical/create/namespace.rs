// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateNamespace,
	plan::logical::{Compiler, CreateNamespaceNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_namespace(&self, ast: AstCreateNamespace<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateNamespace(CreateNamespaceNode {
			segments: ast.namespace.segments,
			if_not_exists: ast.if_not_exists,
		}))
	}
}
