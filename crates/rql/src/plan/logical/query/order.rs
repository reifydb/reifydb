// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{SortDirection, SortKey};

use crate::{
	ast::AstSort,
	plan::logical::{Compiler, LogicalPlan, OrderNode},
};

impl Compiler {
	pub(crate) fn compile_sort(&self, ast: AstSort) -> crate::Result<LogicalPlan> {
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
