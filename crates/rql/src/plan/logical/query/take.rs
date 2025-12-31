// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstTake,
	plan::logical::{Compiler, LogicalPlan, TakeNode},
};

impl Compiler {
	pub(crate) fn compile_take<'a, T: CatalogQueryTransaction>(
		ast: AstTake,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Take(TakeNode {
			take: ast.take,
		}))
	}
}
