// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstCreateHandler,
	plan::logical::{Compiler, CreateHandlerNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_handler(&self, ast: AstCreateHandler<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateHandler(CreateHandlerNode {
			name: ast.name,
			on_event: ast.on_event,
			on_variant: ast.on_variant,
			body_source: ast.body_source,
		}))
	}
}
