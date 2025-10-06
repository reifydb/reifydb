// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogQueryTransaction, store::view::ViewColumnToCreate};
use reifydb_type::Fragment;

use crate::{
	ast::{AstCreateDeferredView, AstDataType},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateDeferredViewNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_deferred_view<'a, T: CatalogQueryTransaction>(
		ast: AstCreateDeferredView<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];
		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let constraint = convert_data_type_with_constraints(&col.ty)?;

			let ty_fragment = match &col.ty {
				AstDataType::Unconstrained(fragment) => fragment.clone(),
				AstDataType::Constrained {
					name,
					..
				} => name.clone(),
			};

			let fragment = Some(Fragment::merge_all([col.name.clone(), ty_fragment]).into_owned());

			columns.push(ViewColumnToCreate {
				name: column_name,
				constraint,
				fragment,
			});
		}

		// Use the view identifier directly from AST
		let view = ast.view;

		let with = if let Some(as_statement) = ast.as_clause {
			Compiler::compile(as_statement, tx)?
		} else {
			vec![]
		};

		Ok(LogicalPlan::CreateDeferredView(CreateDeferredViewNode {
			view,
			if_not_exists: false,
			columns,
			with,
		}))
	}
}
