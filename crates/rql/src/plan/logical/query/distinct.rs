// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_distinct<'a, T: CatalogQueryTransaction>(
		ast: AstDistinct<'a>,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
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
