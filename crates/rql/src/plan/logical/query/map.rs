// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstMap,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LogicalPlan, MapNode},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_map(&self, ast: AstMap<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Map(MapNode {
			map: ast.nodes.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?,
		}))
	}
}
