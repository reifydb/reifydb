// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstAggregate,
	expression::ExpressionCompiler,
	plan::logical::{AggregateNode, Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_aggregate<'a, T: CatalogQueryTransaction>(
		ast: AstAggregate,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Aggregate(AggregateNode {
			by: ast.by.into_iter().map(ExpressionCompiler::compile).collect::<crate::Result<Vec<_>>>()?,
			map: ast.map.into_iter().map(ExpressionCompiler::compile).collect::<crate::Result<Vec<_>>>()?,
		}))
	}
}
