// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstApply;
use reifydb_value::error::ValueError;

use crate::{
	expression::ExpressionCompiler,
	plan::logical::{ApplyNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_apply(&self, ast: AstApply<'bump>) -> Result<LogicalPlan<'bump>, ValueError> {
		let params = ast
			.params
			.into_iter()
			.map(ExpressionCompiler::compile)
			.collect::<Result<Vec<_>, ValueError>>()?;
		let with = Self::compile_apply_with(ast.with.as_ref())?;
		Ok(LogicalPlan::Apply(ApplyNode {
			operator: ast.operator.into_fragment(),
			params,
			with,
			rql: ast.rql.to_string(),
		}))
	}
}
