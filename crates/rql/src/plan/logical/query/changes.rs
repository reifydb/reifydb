// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstChanges,
	plan::logical::{ChangesNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_changes(&self, ast: AstChanges<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Changes(ChangesNode {
			with: Self::compile_changes_with(&ast.token, ast.with.as_ref())?,
			fragment: ast.token.fragment.to_owned(),
		}))
	}
}
