// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstFilter,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, FilterNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_filter<T: CatalogQueryTransaction>(
		ast: AstFilter,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Filter(FilterNode {
			condition: ExpressionCompiler::compile(*ast.node)?,
		}))
	}
}
