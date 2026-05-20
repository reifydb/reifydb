// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
