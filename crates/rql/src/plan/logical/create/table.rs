// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{
	catalog::table::TableColumnToCreate,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::constraint::TypeConstraint};

use crate::{
	ast::ast::{AstColumnProperty, AstCreateTable},
	convert_data_type_with_constraints,
	diagnostic::AstError,
	plan::logical::{Compiler, CreateTableNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_table(
		&self,
		ast: AstCreateTable<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let mut columns: Vec<TableColumnToCreate> = vec![];

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
						None => {
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Enum,
								namespace: ns_name.to_string(),
								name: type_name.to_string(),
								fragment: Fragment::merge_all([
									namespace.to_owned(),
									name.to_owned(),
								]),
							}
							.into());
						}
					}
				}
				_ => match convert_data_type_with_constraints(&col.ty) {
					Ok(c) => c,
					Err(_) => {
						return Err(AstError::UnrecognizedType {
							fragment: col.ty.name_fragment().to_owned(),
						}
						.into());
					}
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
						let dict_namespace_name = if dict_ident.namespace.is_empty() {
							table_namespace_name.to_string()
						} else {
							dict_ident
								.namespace
								.iter()
								.map(|n| n.text())
								.collect::<Vec<_>>()
								.join(".")
						};
						let dict_name = dict_ident.name.text();

						let Some(namespace) = self
							.catalog
							.find_namespace_by_name(tx, &dict_namespace_name)?
						else {
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Dictionary,
								namespace: dict_namespace_name.to_string(),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						let Some(dictionary) = self.catalog.find_dictionary_by_name(
							tx,
							namespace.id,
							dict_name,
						)?
						else {
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Dictionary,
								namespace: dict_namespace_name.to_string(),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						if column_type != dictionary.value_type {
							return Err(CatalogError::DictionaryTypeMismatch {
								column: column_name.clone(),
								column_type,
								dictionary: dict_name.to_string(),
								dictionary_value_type: dictionary.value_type,
								fragment: col.name.to_owned(),
							}
							.into());
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

		let table = ast.table;

		Ok(LogicalPlan::CreateTable(CreateTableNode {
			table,
			if_not_exists: false,
			columns,
		}))
	}
}
