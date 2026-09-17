// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstAggregate,
	expression::ExpressionCompiler,
	plan::logical::{AggregateNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_aggregate(&self, ast: AstAggregate<'bump>) -> Result<LogicalPlan<'bump>> {
		let by = ast.by.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?;
		let map = ast.map.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?;
		let with = Self::compile_aggregate_with(ast.with.as_ref())?;
		Ok(LogicalPlan::Aggregate(AggregateNode {
			by,
			map,
			with,
			rql: ast.rql.to_string(),
		}))
	}
}
