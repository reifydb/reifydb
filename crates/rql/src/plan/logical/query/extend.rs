// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstExtend,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ExtendNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_extend<'a, T: CatalogQueryTransaction>(
		ast: AstExtend,
		_tx: &mut T,
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
