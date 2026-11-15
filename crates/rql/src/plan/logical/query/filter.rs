// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::{ast::ast::AstFilter, bump::BumpBox};
use reifydb_value::error::ValueError;

use crate::{
	expression::ExpressionCompiler,
	plan::logical::{Compiler, FilterNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_filter(&self, ast: AstFilter<'bump>) -> Result<LogicalPlan<'bump>, ValueError> {
		Ok(LogicalPlan::Filter(FilterNode {
			condition: ExpressionCompiler::compile(BumpBox::into_inner(ast.node))?,
			rql: ast.rql.to_string(),
		}))
	}
}
