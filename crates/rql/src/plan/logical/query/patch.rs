// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstPatch;
use reifydb_value::error::ValueError;

use crate::{
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LogicalPlan, PatchNode, reserved::reject_reserved_output_names},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_patch(&self, ast: AstPatch<'bump>) -> Result<LogicalPlan<'bump>, ValueError> {
		let assignments = ast
			.assignments
			.into_iter()
			.map(ExpressionCompiler::compile)
			.collect::<Result<Vec<_>, ValueError>>()?;
		reject_reserved_output_names(&assignments)?;
		Ok(LogicalPlan::Patch(PatchNode {
			assignments,
			rql: ast.rql.to_string(),
		}))
	}
}
