// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstApply,
	expression::ExpressionCompiler,
	plan::logical::{ApplyNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_apply(&self, ast: AstApply<'bump>) -> Result<LogicalPlan<'bump>> {
		let params = ast.params.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?;
		let with = Self::compile_apply_with(ast.with.as_ref())?;
		Ok(LogicalPlan::Apply(ApplyNode {
			operator: ast.operator.into_fragment(),
			params,
			with,
			rql: ast.rql.to_string(),
		}))
	}
}
