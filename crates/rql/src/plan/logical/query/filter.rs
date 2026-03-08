// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstFilter,
	bump::BumpBox,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, FilterNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_filter(&self, ast: AstFilter<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Filter(FilterNode {
			condition: ExpressionCompiler::compile(BumpBox::into_inner(ast.node))?,
			rql: ast.rql.to_string(),
		}))
	}
}
