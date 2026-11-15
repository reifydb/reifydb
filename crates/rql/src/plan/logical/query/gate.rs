// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::{ast::ast::AstGate, bump::BumpBox};
use reifydb_value::error::ValueError;

use crate::{
	expression::ExpressionCompiler,
	plan::logical::{Compiler, GateNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_gate(&self, ast: AstGate<'bump>) -> Result<LogicalPlan<'bump>, ValueError> {
		Ok(LogicalPlan::Gate(GateNode {
			condition: ExpressionCompiler::compile(BumpBox::into_inner(ast.node))?,
			rql: ast.rql.to_string(),
		}))
	}
}
