// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstApply,
	expression::ExpressionCompiler,
	plan::logical::{ApplyNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_apply(&self, ast: AstApply<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Apply(ApplyNode {
			operator: ast.operator.into_fragment(),
			arguments: ast
				.expressions
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<Result<Vec<_>>>()?,
		}))
	}
}
