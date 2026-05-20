// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateBinding,
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
