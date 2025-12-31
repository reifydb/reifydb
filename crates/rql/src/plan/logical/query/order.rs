// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{SortDirection, SortKey};

use crate::{
	ast::AstSort,
	plan::logical::{Compiler, LogicalPlan, OrderNode},
};

impl Compiler {
	pub(crate) fn compile_sort<'a, T: CatalogQueryTransaction>(
		ast: AstSort,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan> {
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
						column: column.name,
						direction,
					}
				})
				.collect(),
		}))
	}
}
