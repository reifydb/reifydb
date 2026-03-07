// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateEvent,
	plan::logical::{Compiler, CreateEventNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_event(&self, ast: AstCreateEvent<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateEvent(CreateEventNode {
			name: ast.name,
			variants: ast.variants,
		}))
	}
}
