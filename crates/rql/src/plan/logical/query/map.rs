// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::error::diagnostic::query::empty_map;
use reifydb_value::return_error;

use crate::{
	Result,
	ast::ast::AstMap,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LogicalPlan, MapNode, reserved::reject_reserved_output_names},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_map(&self, ast: AstMap<'bump>) -> Result<LogicalPlan<'bump>> {
		if ast.nodes.is_empty() {
			return_error!(empty_map(ast.token.fragment.to_owned()));
		}
		let map = ast.nodes.into_iter().map(ExpressionCompiler::compile).collect::<Result<Vec<_>>>()?;
		reject_reserved_output_names(&map)?;
		Ok(LogicalPlan::Map(MapNode {
			map,
			rql: ast.rql.to_string(),
		}))
	}
}
