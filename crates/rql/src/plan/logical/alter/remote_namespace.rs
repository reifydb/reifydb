// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstAlterRemoteNamespace,
	plan::logical::{AlterRemoteNamespaceNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_remote_namespace(
		&self,
		ast: AstAlterRemoteNamespace<'bump>,
	) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::AlterRemoteNamespace(AlterRemoteNamespaceNode {
			namespace: ast.namespace.segments,
			grpc: ast.grpc,
		}))
	}
}
