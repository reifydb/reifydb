// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::view::ViewColumnToCreate;
use reifydb_type::Fragment;

use crate::{
	ast::{AstCreateTransactionalView, AstDataType},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateTransactionalViewNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_transactional_view<'a>(
		ast: AstCreateTransactionalView<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];
		for col in ast.columns.into_iter() {
			let column_name = col.name.value().to_string();
			let constraint =
				convert_data_type_with_constraints(&col.ty)?;

			let ty_fragment = match &col.ty {
				AstDataType::Simple(ident) => {
					ident.0.fragment.clone()
				}
				AstDataType::WithParams {
					name,
					..
				} => name.0.fragment.clone(),
			};

			let fragment = Some(Fragment::merge_all([
				col.name.0.fragment.clone(),
				ty_fragment,
			])
			.into_owned());

			columns.push(ViewColumnToCreate {
				name: column_name,
				constraint,
				fragment,
			});
		}

		let with = if let Some(with_statement) = ast.with {
			Self::compile(with_statement)?
		} else {
			vec![]
		};

		Ok(LogicalPlan::CreateTransactionalView(
			CreateTransactionalViewNode {
				schema: ast.schema.fragment(),
				view: ast.view.fragment(),
				if_not_exists: false,
				columns,
				with,
			},
		))
	}
}
