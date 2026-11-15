// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstExtend;
use reifydb_value::error::ValueError;

use crate::{
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ExtendNode, LogicalPlan, reserved::reject_reserved_output_names},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_extend(&self, ast: AstExtend<'bump>) -> Result<LogicalPlan<'bump>, ValueError> {
		let extend = ast
			.nodes
			.into_iter()
			.map(ExpressionCompiler::compile)
			.collect::<Result<Vec<_>, ValueError>>()?;
		reject_reserved_output_names(&extend)?;
		Ok(LogicalPlan::Extend(ExtendNode {
			extend,
			rql: ast.rql.to_string(),
		}))
	}
}
