// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::view::ViewColumnToCreate;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::ast::AstCreateTransactionalView,
	bump::BumpVec,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateTransactionalViewNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_transactional_view<T: AsTransaction>(
		&self,
		ast: AstCreateTransactionalView<'bump>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
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

		// Convert AST primary key to logical plan primary key
		let primary_key = ast.primary_key.map(|pk| {
			use crate::plan::logical::{PrimaryKeyColumn, PrimaryKeyDef};

			PrimaryKeyDef {
				columns: pk
					.columns
					.into_iter()
					.map(|col| PrimaryKeyColumn {
						column: col.column.name,
						order: col.order,
					})
					.collect(),
			}
		});

		Ok(LogicalPlan::CreateTransactionalView(CreateTransactionalViewNode {
			view,
			if_not_exists: false,
			columns,
			as_clause: with,
			primary_key,
		}))
	}
}
