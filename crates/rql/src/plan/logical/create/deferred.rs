// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogQueryTransaction, view::ViewColumnToCreate};
use reifydb_type::Fragment;

use crate::{
	ast::{AstCreateDeferredView, AstDataType},
	convert_data_type_with_constraints,
	plan::logical::{
		Compiler, CreateDeferredViewNode, LogicalPlan,
		resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_deferred_view<
		'a,
		't,
		T: CatalogQueryTransaction,
	>(
		ast: AstCreateDeferredView<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];
		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let constraint =
				convert_data_type_with_constraints(&col.ty)?;

			let ty_fragment = match &col.ty {
				AstDataType::Simple(fragment) => {
					fragment.clone()
				}
				AstDataType::WithConstraints {
					name,
					..
				} => name.clone(),
			};

			let fragment = Some(Fragment::merge_all([
				col.name.clone(),
				ty_fragment,
			])
			.into_owned());

			columns.push(ViewColumnToCreate {
				name: column_name,
				constraint,
				fragment,
			});
		}

		// Convert MaybeQualified to fully qualified using resolver
		// Don't validate existence since we're creating the view
		let view = resolver.resolve_maybe_source_with_validation(
			&ast.view, false,
		)?;

		let with = if let Some(as_statement) = ast.as_clause {
			Compiler::compile(as_statement, resolver)?
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
