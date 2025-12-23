// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
