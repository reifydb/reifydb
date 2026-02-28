// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateSumType,
	plan::logical::{Compiler, CreateSumTypeNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_sumtype(&self, ast: AstCreateSumType<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateSumType(CreateSumTypeNode {
			name: ast.name,
			if_not_exists: ast.if_not_exists,
			variants: ast.variants,
		}))
	}
}
