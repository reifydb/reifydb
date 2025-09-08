// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstExtend,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ExtendNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_extend(
		ast: AstExtend,
	) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Extend(ExtendNode {
			extend: ast
				.nodes
				.into_iter()
				.map(ExpressionCompiler::compile)
				.collect::<crate::Result<Vec<_>>>()?,
		}))
	}
}
