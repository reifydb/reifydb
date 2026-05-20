// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
