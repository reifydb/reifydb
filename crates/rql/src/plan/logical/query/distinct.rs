// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_distinct(&self, ast: AstDistinct<'bump>) -> crate::Result<LogicalPlan<'bump>> {
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
