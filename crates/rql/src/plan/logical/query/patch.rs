// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstPatch,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LogicalPlan, PatchNode, reserved::reject_reserved_output_names},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_patch(&self, ast: AstPatch<'bump>) -> Result<LogicalPlan<'bump>> {
		let assignments =
			ast.assignments.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?;
		reject_reserved_output_names(&assignments)?;
		Ok(LogicalPlan::Patch(PatchNode {
			assignments,
			rql: ast.rql.to_string(),
		}))
	}
}
