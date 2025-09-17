// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{SortDirection, SortKey};

use crate::{
	ast::AstSort,
	plan::logical::{Compiler, LogicalPlan, OrderNode, resolver::IdentifierResolver},
};

impl Compiler {
	pub(crate) fn compile_sort<'a, 't, T: CatalogQueryTransaction>(
		ast: AstSort<'a>,
		_resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Order(OrderNode {
			by: ast.columns
				.into_iter()
				.zip(ast.directions)
				.map(|(column, direction)| {
					let direction = direction
						.map(|direction| match direction.text().to_lowercase().as_str() {
							"asc" => SortDirection::Asc,
							_ => SortDirection::Desc,
						})
						.unwrap_or(SortDirection::Desc);

					SortKey {
						column: column.name.into_owned(),
						direction,
					}
				})
				.collect(),
		}))
	}
}
