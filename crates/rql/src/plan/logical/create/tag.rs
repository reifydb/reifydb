// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateTag,
	plan::logical::{Compiler, CreateTagNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_tag(&self, ast: AstCreateTag<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateTag(CreateTagNode {
			name: ast.name,
			variants: ast.variants,
		}))
	}
}
