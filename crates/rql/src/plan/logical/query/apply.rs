// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstApply,
	expression::ExpressionCompiler,
	plan::logical::{ApplyNode, Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_apply(
		ast: AstApply,
	) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Apply(ApplyNode {
			operator_name: ast.operator_name.into_fragment(),
			arguments: ast
				.expressions
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<crate::Result<Vec<_>>>()?,
		}))
	}
}
