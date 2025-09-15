// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogQueryTransaction, table::TableColumnToCreate};
use reifydb_core::interface::ColumnPolicyKind;
use reifydb_type::Fragment;

use crate::{
	ast::AstCreateTable,
	convert_data_type_with_constraints,
	plan::logical::{
		Compiler, CreateTableNode, LogicalPlan, convert_policy,
		resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_create_table<
		'a,
		't,
		T: CatalogQueryTransaction,
	>(
		ast: AstCreateTable<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<TableColumnToCreate> = vec![];

		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let constraint =
				convert_data_type_with_constraints(&col.ty)?;

			let policies = if let Some(policy_block) = &col.policies
			{
				policy_block
					.policies
					.iter()
					.map(convert_policy)
					.collect::<Vec<ColumnPolicyKind>>()
			} else {
				vec![]
			};

			let ty_fragment = match &col.ty {
				crate::ast::AstDataType::Simple(fragment) => {
					fragment.clone()
				}
				crate::ast::AstDataType::WithConstraints {
					name,
					..
				} => name.clone(),
			};

			let fragment = Some(Fragment::merge_all([
				col.name.clone(),
				ty_fragment,
			])
			.into_owned());

			columns.push(TableColumnToCreate {
				name: column_name,
				constraint,
				policies,
				auto_increment: col.auto_increment,
				fragment,
			});
		}

		// Resolve directly to TableIdentifier
		// Don't validate existence since we're creating the table
		let table = resolver
			.resolve_maybe_qualified_table(&ast.table, false)?;

		Ok(LogicalPlan::CreateTable(CreateTableNode {
			table,
			if_not_exists: false,
			columns,
		}))
	}
}
