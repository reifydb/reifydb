// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogQueryTransaction, store::view::ViewColumnToCreate};
use reifydb_type::Fragment;

use crate::{
	ast::{AstCreateFlow, AstDataType},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateFlowNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_flow<'a, T: CatalogQueryTransaction>(
		ast: AstCreateFlow<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];

		// Process columns if provided (optional for flows)
		if let Some(cols) = ast.columns {
			for col in cols.into_iter() {
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
		}

		// Use the flow identifier directly from AST
		let flow = ast.flow;

		// Compile the AS clause (required for flows)
		let with = Compiler::compile(ast.as_clause, tx)?;

		Ok(LogicalPlan::CreateFlow(CreateFlowNode {
			flow,
			if_not_exists: ast.if_not_exists,
			columns,
			with,
		}))
	}
}
