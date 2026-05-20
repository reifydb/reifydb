// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateSink,
	plan::logical::{Compiler, CreateSinkNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_sink(&self, ast: AstCreateSink<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateSink(CreateSinkNode {
			name: ast.name,
			source: ast.source,
			connector: ast.connector,
			config: ast.config,
		}))
	}
}
