// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::view::ViewColumnToCreate;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::ast::{AstCreateTransactionalView, AstDataType},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateTransactionalViewNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_transactional_view<T: AsTransaction>(
		&self,
		ast: AstCreateTransactionalView,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
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

			let fragment = Some(Fragment::merge_all([col.name.clone(), ty_fragment]));

			columns.push(ViewColumnToCreate {
				name: column_name,
				constraint,
				fragment,
			});
		}

		// Use the view identifier directly from AST
		let view = ast.view;

		let with = if let Some(as_statement) = ast.as_clause {
			self.compile(as_statement, tx)?
		} else {
			vec![]
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
