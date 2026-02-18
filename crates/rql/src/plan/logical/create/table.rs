// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::table::TableColumnToCreate;
use reifydb_core::{
	error::diagnostic::catalog::{dictionary_not_found, dictionary_type_mismatch, sumtype_not_found},
	interface::catalog::policy::ColumnPolicyKind,
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{
	error::diagnostic::ast::unrecognized_type, fragment::Fragment, return_error, value::constraint::TypeConstraint,
};

use crate::{
	ast::ast::AstCreateTable,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateTableNode, LogicalPlan, convert_policy},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_table<T: AsTransaction>(
		&self,
		ast: AstCreateTable<'bump>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
		let mut columns: Vec<TableColumnToCreate> = vec![];

		// Get the table's namespace for dictionary resolution
		let table_namespace_name = ast.table.namespace.first().map(|n| n.text()).unwrap_or("default");

		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let constraint = match &col.ty {
				crate::ast::ast::AstType::Qualified {
					namespace,
					name,
				} => {
					let ns_name = namespace.text();
					let type_name = name.text();
					let ns = self.catalog.find_namespace_by_name(tx, ns_name)?;
					let sumtype = ns
						.and_then(|ns| {
							self.catalog
								.find_sumtype_by_name(tx, ns.id, type_name)
								.transpose()
						})
						.transpose()?;
					match sumtype {
						Some(def) => TypeConstraint::sumtype(def.id),
						None => return_error!(sumtype_not_found(
							Fragment::merge_all([namespace.to_owned(), name.to_owned()]),
							ns_name,
							type_name,
						)),
					}
				}
				_ => match convert_data_type_with_constraints(&col.ty) {
					Ok(c) => c,
					Err(_) => return_error!(unrecognized_type(col.ty.name_fragment().to_owned())),
				},
			};
			let column_type = constraint.get_type();

			let policies = if let Some(policy_block) = &col.policies {
				policy_block.policies.iter().map(convert_policy).collect::<Vec<ColumnPolicyKind>>()
			} else {
				vec![]
			};

			let name = col.name.to_owned();
			let ty_fragment = col.ty.name_fragment().to_owned();
			let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

			// Resolve dictionary if specified
			let dictionary_id = if let Some(ref dict_ident) = col.dictionary {
				// Get the dictionary's namespace (uses column's namespace or table's namespace)
				let dict_namespace_name =
					dict_ident.namespace.first().map(|n| n.text()).unwrap_or(table_namespace_name);
				let dict_name = dict_ident.name.text();

				// Find the namespace
				let Some(namespace) = self.catalog.find_namespace_by_name(tx, dict_namespace_name)?
				else {
					return_error!(dictionary_not_found(
						dict_ident.name.to_owned(),
						dict_namespace_name,
						dict_name,
					));
				};

				// Find the dictionary
				let Some(dictionary) =
					self.catalog.find_dictionary_by_name(tx, namespace.id, dict_name)?
				else {
					return_error!(dictionary_not_found(
						dict_ident.name.to_owned(),
						dict_namespace_name,
						dict_name,
					));
				};

				// Validate type compatibility: column type must match dictionary's value_type
				if column_type != dictionary.value_type {
					return_error!(dictionary_type_mismatch(
						col.name.to_owned(),
						&column_name,
						column_type,
						dict_name,
						dictionary.value_type,
					));
				}

				Some(dictionary.id)
			} else {
				None
			};

			columns.push(TableColumnToCreate {
				name,
				fragment,
				constraint,
				policies,
				auto_increment: col.auto_increment,
				dictionary_id,
			});
		}

		// Use the table identifier directly from AST
		let table = ast.table;

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

		Ok(LogicalPlan::CreateTable(CreateTableNode {
			table,
			if_not_exists: false,
			columns,
			primary_key,
		}))
	}
}
