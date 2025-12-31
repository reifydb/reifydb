// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::AstMap,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LogicalPlan, MapNode},
};

impl Compiler {
	pub(crate) fn compile_map(&self, ast: AstMap) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Map(MapNode {
			map: ast.nodes
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<crate::Result<Vec<_>>>()?,
		}))
	}
}
