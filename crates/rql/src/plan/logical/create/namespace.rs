// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::{AstCreateNamespace, AstCreateRemoteNamespace},
	plan::logical::{Compiler, CreateNamespaceNode, CreateRemoteNamespaceNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_namespace(&self, ast: AstCreateNamespace<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateNamespace(CreateNamespaceNode {
			segments: ast.namespace.segments,
			if_not_exists: ast.if_not_exists,
		}))
	}

	pub(crate) fn compile_create_remote_namespace(
		&self,
		ast: AstCreateRemoteNamespace<'bump>,
	) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateRemoteNamespace(CreateRemoteNamespaceNode {
			segments: ast.namespace.segments,
			if_not_exists: ast.if_not_exists,
			grpc: ast.grpc,
		}))
	}
}
