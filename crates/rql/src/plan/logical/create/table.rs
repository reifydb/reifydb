// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::table::TableColumnToCreate;
use reifydb_core::interface::ColumnPolicyKind;

use crate::{
	ast::AstCreateTable,
	convert_data_type_with_constraints,
	plan::logical::{
		Compiler, CreateTableNode, LogicalPlan, convert_policy,
	},
};

impl Compiler {
	pub(crate) fn compile_create_table<'a>(
		ast: AstCreateTable<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<TableColumnToCreate> = vec![];

		for col in ast.columns.into_iter() {
			let column_name = col.name.value().to_string();
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
				crate::ast::AstDataType::Simple(ident) => {
					ident.0.fragment.clone()
				}
				crate::ast::AstDataType::WithParams {
					name,
					..
				} => name.0.fragment.clone(),
			};

			let fragment = Some(Fragment::merge_all([
				col.name.0.fragment.clone(),
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

		// Convert MaybeQualified to fully qualified
		use reifydb_core::interface::identifier::SourceIdentifier;
		use reifydb_type::{Fragment, OwnedFragment};

		let schema = ast.table.schema.unwrap_or_else(|| {
			Fragment::Owned(OwnedFragment::Internal {
				text: String::from("default"),
			})
		});

		let mut table = SourceIdentifier::new(
			schema,
			ast.table.name,
			ast.table.kind,
		);
		if let Some(alias) = ast.table.alias {
			table = table.with_alias(alias);
		}

		Ok(LogicalPlan::CreateTable(CreateTableNode {
			table,
			if_not_exists: false,
			columns,
		}))
	}
}
