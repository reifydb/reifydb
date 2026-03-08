// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstAggregate,
	expression::ExpressionCompiler,
	plan::logical::{AggregateNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_aggregate(&self, ast: AstAggregate<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Aggregate(AggregateNode {
			by: ast.by.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?,
			map: ast.map.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?,
			rql: ast.rql.to_string(),
		}))
	}
}
