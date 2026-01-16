// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstExtend,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ExtendNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_extend(&self, ast: AstExtend) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Extend(ExtendNode {
			extend: ast
				.nodes
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<crate::Result<Vec<_>>>()?,
		}))
	}
}
