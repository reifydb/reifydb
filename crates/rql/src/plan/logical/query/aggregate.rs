// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstAggregate;
use reifydb_value::error::ValueError;

use crate::{
	expression::ExpressionCompiler,
	plan::logical::{AggregateNode, Compiler, LogicalPlan, reserved::reject_reserved_output_names},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_aggregate(&self, ast: AstAggregate<'bump>) -> Result<LogicalPlan<'bump>, ValueError> {
		let by = ast.by.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>, ValueError>>()?;
		let map =
			ast.map.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>, ValueError>>()?;
		reject_reserved_output_names(&map)?;
		let with = Self::compile_aggregate_with(ast.with.as_ref())?;
		Ok(LogicalPlan::Aggregate(AggregateNode {
			by,
			map,
			with,
			rql: ast.rql.to_string(),
		}))
	}
}
