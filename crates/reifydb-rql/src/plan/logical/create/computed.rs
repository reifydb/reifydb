// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::view::ViewColumnToCreate;

use crate::{
	ast::AstCreateComputedView,
	convert_data_type,
	plan::logical::{Compiler, CreateComputedViewNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_computed_view(
		ast: AstCreateComputedView,
	) -> crate::Result<LogicalPlan> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];
		for col in ast.columns.into_iter() {
			let column_name = col.name.value().to_string();
			let column_type = convert_data_type(&col.ty)?;

			columns.push(ViewColumnToCreate {
				name: column_name,
				ty: column_type,
				span: Some(col.name.span()),
			});
		}

		// Compile the WITH clause if present
		let with = if let Some(with_statement) = ast.with {
			let compiled_plans = Self::compile(with_statement)?;
			Some(compiled_plans)
		} else {
			None
		};

		Ok(LogicalPlan::CreateComputedView(CreateComputedViewNode {
			schema: ast.schema.span(),
			view: ast.view.span(),
			if_not_exists: false,
			columns,
			with,
		}))
	}
}
