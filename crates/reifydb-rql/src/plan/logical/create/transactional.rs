// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::view::ViewColumnToCreate;

use crate::{
	ast::AstCreateTransactionalView,
	convert_data_type,
	plan::logical::{Compiler, CreateTransactionalViewNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_transactional_view<'a>(
		ast: AstCreateTransactionalView<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];
		for col in ast.columns.into_iter() {
			let column_name = col.name.value().to_string();
			let column_type = convert_data_type(&col.ty)?;

			columns.push(ViewColumnToCreate {
				name: column_name,
				ty: column_type,
				fragment: Some(col
					.name
					.fragment()
					.into_owned()),
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
