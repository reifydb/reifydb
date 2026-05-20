// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateSource,
	plan::logical::{Compiler, CreateSourceNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_source(&self, ast: AstCreateSource<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateSource(CreateSourceNode {
			name: ast.name,
			connector: ast.connector,
			config: ast.config,
			target: ast.target,
		}))
	}
}
