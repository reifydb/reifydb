// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstExtend,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ExtendNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_extend(&self, ast: AstExtend<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Extend(ExtendNode {
			extend: ast.nodes.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?,
			rql: ast.rql.to_string(),
		}))
	}
}
