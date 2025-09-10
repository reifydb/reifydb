// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnSource};
use reifydb_type::{Fragment, OwnedFragment};

use crate::{
	ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_distinct<'a>(
		ast: AstDistinct<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		// DISTINCT operates on the output columns of the query
		// In a proper implementation, we would need to resolve these
		// columns based on the SELECT clause and FROM sources in the
		// query context For now, we'll create columns with a default
		// schema/source that should be resolved by the query planner
		// based on context

		Ok(LogicalPlan::Distinct(DistinctNode {
			columns: ast
				.columns
				.into_iter()
				.map(|col| {
					// TODO: This should be resolved from
					// the query context For now, using
					// a placeholder that indicates
					// resolution needed The physical
					// plan compiler should resolve this
					// based on the actual tables/views
					// being queried
					ColumnIdentifier {
						source: ColumnSource::Source {
							schema: Fragment::Owned(
								OwnedFragment::Internal { text: String::from("_context") }
							),
							source: Fragment::Owned(
								OwnedFragment::Internal { text: String::from("_context") }
							),
						},
						name: col.fragment(),
					}
				})
				.collect(),
		}))
	}
}
