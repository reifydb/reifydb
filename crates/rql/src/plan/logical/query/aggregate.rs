// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstAggregate,
	expression::ExpressionCompiler,
	plan::logical::{AggregateNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_aggregate(&self, ast: AstAggregate<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Aggregate(AggregateNode {
			by: ast.by.into_iter().map(ExpressionCompiler::compile).collect::<crate::Result<Vec<_>>>()?,
			map: ast.map.into_iter().map(ExpressionCompiler::compile).collect::<crate::Result<Vec<_>>>()?,
		}))
	}
}
