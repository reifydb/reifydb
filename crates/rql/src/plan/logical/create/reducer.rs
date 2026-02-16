// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::reducer::ReducerColumnToCreate;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::ast::AstCreateReducer,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateReducerNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_reducer<T: AsTransaction>(
		&self,
		ast: AstCreateReducer<'bump>,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
		let mut columns = Vec::new();

		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let constraint = convert_data_type_with_constraints(&col.ty)?;

			let ty_fragment = match &col.ty {
				crate::ast::ast::AstType::Unconstrained(fragment) => fragment.to_owned(),
				crate::ast::ast::AstType::Constrained {
					name,
					..
				} => name.to_owned(),
			};

			let fragment = Some(Fragment::merge_all([col.name.to_owned(), ty_fragment]));

			columns.push(ReducerColumnToCreate {
				name: column_name,
				constraint,
				fragment,
			});
		}

		Ok(LogicalPlan::CreateReducer(CreateReducerNode {
			reducer: ast.name,
			columns,
			key: ast.key,
		}))
	}
}
