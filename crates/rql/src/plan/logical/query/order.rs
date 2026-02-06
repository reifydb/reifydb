// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::sort::{SortDirection, SortKey};

use crate::{
	ast::ast::AstSort,
	plan::logical::{Compiler, LogicalPlan, OrderNode},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_sort(&self, ast: AstSort<'bump>) -> crate::Result<LogicalPlan<'bump>> {
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
						column: column.name.to_owned(),
						direction,
					}
				})
				.collect(),
		}))
	}
}
