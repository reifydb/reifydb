// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstApply,
	expression::ExpressionCompiler,
	plan::logical::{ApplyNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_apply(&self, ast: AstApply<'bump>) -> Result<LogicalPlan<'bump>> {
		let ttl = match ast.ttl {
			Some(ast_ttl) => Some(Self::compile_operator_ttl(ast_ttl)?),
			None => None,
		};
		Ok(LogicalPlan::Apply(ApplyNode {
			operator: ast.operator.into_fragment(),
			arguments: ast
				.expressions
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<Result<Vec<_>>>()?,
			ttl,
			rql: ast.rql.to_string(),
		}))
	}
}
