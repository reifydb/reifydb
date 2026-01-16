// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::sort::{SortDirection, SortKey};

use crate::{
	ast::ast::AstSort,
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
