// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::view::ViewColumnToCreate;

use crate::{
	ast::AstCreateTransactionalView,
	convert_data_type,
	plan::logical::{Compiler, CreateTransactionalViewNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_transactional_view(
		ast: AstCreateTransactionalView,
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

		let with = if let Some(with_statement) = ast.with {
			Self::compile(with_statement)?
		} else {
			vec![]
		};

		Ok(LogicalPlan::CreateTransactionalView(
			CreateTransactionalViewNode {
				schema: ast.schema.span(),
				view: ast.view.span(),
				if_not_exists: false,
				columns,
				with,
			},
		))
	}
}
