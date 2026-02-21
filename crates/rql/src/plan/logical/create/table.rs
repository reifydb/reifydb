// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::table::TableColumnToCreate;
use reifydb_core::error::diagnostic::catalog::{dictionary_not_found, dictionary_type_mismatch, sumtype_not_found};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::diagnostic::ast::unrecognized_type, fragment::Fragment, return_error, value::constraint::TypeConstraint,
};

use crate::{
	ast::ast::{AstColumnProperty, AstCreateTable},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateTableNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_table(
		&self,
		ast: AstCreateTable<'bump>,
		tx: &mut Transaction<'_>,
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

			let name = col.name.to_owned();
			let ty_fragment = col.ty.name_fragment().to_owned();
			let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

			let mut auto_increment = false;
			let mut dictionary_id = None;
			let policies = vec![];

			for property in &col.properties {
				match property {
					AstColumnProperty::AutoIncrement => auto_increment = true,
					AstColumnProperty::Dictionary(dict_ident) => {
						let dict_namespace_name = dict_ident
							.namespace
							.first()
							.map(|n| n.text())
							.unwrap_or(table_namespace_name);
						let dict_name = dict_ident.name.text();

						let Some(namespace) =
							self.catalog.find_namespace_by_name(tx, dict_namespace_name)?
						else {
							return_error!(dictionary_not_found(
								dict_ident.name.to_owned(),
								dict_namespace_name,
								dict_name,
							));
						};

						let Some(dictionary) = self.catalog.find_dictionary_by_name(
							tx,
							namespace.id,
							dict_name,
						)?
						else {
							return_error!(dictionary_not_found(
								dict_ident.name.to_owned(),
								dict_namespace_name,
								dict_name,
							));
						};

						if column_type != dictionary.value_type {
							return_error!(dictionary_type_mismatch(
								col.name.to_owned(),
								&column_name,
								column_type,
								dict_name,
								dictionary.value_type,
							));
						}

						dictionary_id = Some(dictionary.id);
					}
					AstColumnProperty::Saturation(_) => {
						// TODO: inline saturation policy
					}
					AstColumnProperty::Default(_) => {
						// TODO: inline default policy
					}
				}
			}

			columns.push(TableColumnToCreate {
				name,
				fragment,
				constraint,
				policies,
				auto_increment,
				dictionary_id,
			});
		}

		// Use the table identifier directly from AST
		let table = ast.table;

		Ok(LogicalPlan::CreateTable(CreateTableNode {
			table,
			if_not_exists: false,
			columns,
		}))
	}
}
