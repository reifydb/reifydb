// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::view::ViewColumnToCreate;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	ast::ast::AstCreateDeferredView,
	bump::BumpVec,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateDeferredViewNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_deferred_view(
		&self,
		ast: AstCreateDeferredView<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];
		for col in ast.columns.into_iter() {
			let constraint = convert_data_type_with_constraints(&col.ty)?;

			let name = col.name.to_owned();
			let ty_fragment = col.ty.name_fragment().to_owned();
			let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

			columns.push(ViewColumnToCreate {
				name,
				fragment,
				constraint,
			});
		}

		// Use the view identifier directly from AST
		let view = ast.view;

		let with = if let Some(as_statement) = ast.as_clause {
			self.compile(as_statement, tx)?
		} else {
			BumpVec::new_in(self.bump)
		};

		Ok(LogicalPlan::CreateDeferredView(CreateDeferredViewNode {
			view,
			if_not_exists: false,
			columns,
			as_clause: with,
		}))
	}
}
