// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstAlterRemoteNamespace;
use reifydb_value::error::ValueError;

use crate::plan::logical::{AlterRemoteNamespaceNode, Compiler, LogicalPlan};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_remote_namespace(
		&self,
		ast: AstAlterRemoteNamespace<'bump>,
	) -> Result<LogicalPlan<'bump>, ValueError> {
		Ok(LogicalPlan::AlterRemoteNamespace(AlterRemoteNamespaceNode {
			namespace: ast.namespace.segments,
			grpc: ast.grpc,
		}))
	}
}
