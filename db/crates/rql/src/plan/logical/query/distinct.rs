// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnSource};
use reifydb_type::{Fragment, OwnedFragment};

use crate::{
	ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan, resolver::IdentifierResolver},
};

impl Compiler {
	pub(crate) fn compile_distinct<'a, 't, T: CatalogQueryTransaction>(
		ast: AstDistinct<'a>,
		_resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// DISTINCT operates on the output columns of the query
		// In a proper implementation, we would need to resolve these
		// columns based on the SELECT clause and FROM sources in the
		// query context For now, we'll create columns with a default
		// namespace/source that should be resolved by the query planner
		// based on context

		Ok(LogicalPlan::Distinct(DistinctNode {
			columns: ast
				.columns
				.into_iter()
				.map(|col| {
					// Convert MaybeQualifiedColumnIdentifier to fully qualified
					// For now, if it's already qualified, use that info
					// Otherwise use placeholder that needs resolution
					match col.source {
						crate::ast::identifier::MaybeQualifiedColumnSource::Source {
							namespace,
							source,
						} => ColumnIdentifier {
							source: ColumnSource::Source {
								namespace: namespace.unwrap_or_else(|| {
									Fragment::Owned(OwnedFragment::Internal {
										text: String::from("_context"),
									})
								}),
								source,
							},
							name: col.name,
						},
						crate::ast::identifier::MaybeQualifiedColumnSource::Alias(alias) => {
							ColumnIdentifier {
								source: ColumnSource::Alias(alias),
								name: col.name,
							}
						}
						crate::ast::identifier::MaybeQualifiedColumnSource::Unqualified => {
							// Unqualified - needs resolution from context
							ColumnIdentifier {
								source: ColumnSource::Source {
									namespace: Fragment::Owned(
										OwnedFragment::Internal {
											text: String::from("_context"),
										},
									),
									source: Fragment::Owned(
										OwnedFragment::Internal {
											text: String::from("_context"),
										},
									),
								},
								name: col.name,
							}
						}
					}
				})
				.collect(),
		}))
	}
}
