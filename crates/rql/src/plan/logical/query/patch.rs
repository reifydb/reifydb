// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstPatch,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LogicalPlan, PatchNode},
};

impl Compiler {
	pub(crate) fn compile_patch(&self, ast: AstPatch) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Patch(PatchNode {
			assignments: ast
				.assignments
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<crate::Result<Vec<_>>>()?,
		}))
	}
}
