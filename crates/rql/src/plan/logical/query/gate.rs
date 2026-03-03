// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstGate,
	bump::BumpBox,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, GateNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_gate(&self, ast: AstGate<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Gate(GateNode {
			condition: ExpressionCompiler::compile(BumpBox::into_inner(ast.node))?,
		}))
	}
}
