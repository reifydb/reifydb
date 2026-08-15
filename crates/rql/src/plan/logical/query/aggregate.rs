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
		let ttl = match ast.ttl {
			Some(ast_ttl) => Some(Self::compile_operator_lateness(ast_ttl)?),
			None => None,
		};

		Ok(LogicalPlan::Aggregate(AggregateNode {
			by: ast.by.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?,
			map: ast.map.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?,
			ttl,
			rql: ast.rql.to_string(),
		}))
	}
}
