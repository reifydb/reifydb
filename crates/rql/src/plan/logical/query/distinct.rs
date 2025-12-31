// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_distinct<'a, T: CatalogQueryTransaction>(
		ast: AstDistinct,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// DISTINCT operates on the output columns of the query
		// In a proper implementation, we would need to resolve these
		// columns based on the SELECT clause and FROM sources in the
		// query context For now, we'll create columns with a default
		// namespace/source that should be resolved by the query planner
		// based on context

		Ok(LogicalPlan::Distinct(DistinctNode {
			columns: ast.columns,
		}))
	}
}
