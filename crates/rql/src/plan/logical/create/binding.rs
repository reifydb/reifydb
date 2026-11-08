// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreateBinding;

use crate::{
	Result,
	plan::logical::{Compiler, CreateBindingNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_binding(&self, ast: AstCreateBinding<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateBinding(CreateBindingNode {
			name: ast.name,
			procedure: ast.procedure,
			protocol: ast.protocol,
		}))
	}
}
