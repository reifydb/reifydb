// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstPatch,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LogicalPlan, PatchNode},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_patch(&self, ast: AstPatch<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Patch(PatchNode {
			assignments: ast
				.assignments
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<Result<Vec<_>>>()?,
		}))
	}
}
