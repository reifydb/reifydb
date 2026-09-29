// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstExtend,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ExtendNode, LogicalPlan, reserved::reject_reserved_output_names},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_extend(&self, ast: AstExtend<'bump>) -> Result<LogicalPlan<'bump>> {
		let extend = ast.nodes.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?;
		reject_reserved_output_names(&extend)?;
		Ok(LogicalPlan::Extend(ExtendNode {
			extend,
			rql: ast.rql.to_string(),
		}))
	}
}
