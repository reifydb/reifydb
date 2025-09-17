// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogStore, primary_key::PrimaryKeyToCreate};
use reifydb_core::{
	interface::{SourceId, Transaction},
	return_error,
	value::columnar::Columns,
};
use reifydb_rql::plan::{logical::alter::AlterViewOperation, physical::AlterViewPlan};
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn execute_alter_view<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: AlterViewPlan,
	) -> crate::Result<Columns> {
		// View is already fully qualified
		let namespace_name = plan.node.view.namespace().text();
		let view_name = plan.node.view.name().text();

		// Find the namespace
		let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name)? else {
			return_error!(reifydb_core::diagnostic::catalog::namespace_not_found(
				Some(plan.node.view.namespace().clone().into_owned()),
				namespace_name,
			));
		};

		// Find the view
		let Some(view) = CatalogStore::find_view_by_name(txn, namespace.id, view_name)? else {
			return_error!(reifydb_core::diagnostic::catalog::view_not_found(
				plan.node.view.name().clone().into_owned(),
				&namespace.name,
				view_name,
			));
		};

		let mut results = Vec::new();

		// Process each operation
		for operation in plan.node.operations {
			match operation {
				AlterViewOperation::CreatePrimaryKey {
					name,
					columns,
				} => {
					// Get all columns for the view to
					// validate and resolve column IDs
					let view_columns = CatalogStore::list_columns(txn, view.id)?;

					// Map column names to IDs
					let mut column_ids = Vec::new();
					for ast_column in columns {
						let column_name = ast_column.column.name.text();

						// Find the column by name
						let Some(column) =
							view_columns.iter().find(|col| col.name == column_name)
						else {
							return_error!(
								reifydb_core::diagnostic::query::column_not_found(
									ast_column.column.name.clone().into_owned()
								)
							);
						};

						column_ids.push(column.id);
					}

					// Create the primary key
					CatalogStore::create_primary_key(
						txn,
						PrimaryKeyToCreate {
							source: SourceId::View(view.id),
							column_ids,
						},
					)?;

					let pk_name = name
						.map(|n| n.text().to_string())
						.unwrap_or_else(|| "unnamed".to_string());

					results.push([
						("operation", Value::Utf8("CREATE PRIMARY KEY".to_string())),
						("namespace", Value::Utf8(namespace.name.clone())),
						("view", Value::Utf8(view.name.clone())),
						("primary_key", Value::Utf8(pk_name)),
					]);
				}
				AlterViewOperation::DropPrimaryKey => {
					// Not implemented per requirements
					continue;
				}
			}
		}

		// Return results for all operations performed
		if results.is_empty() {
			// No operations performed, return empty result
			Ok(Columns::single_row([
				("operation", Value::Utf8("NO OPERATIONS".to_string())),
				("namespace", Value::Utf8(namespace.name)),
				("view", Value::Utf8(view.name)),
			]))
		} else if results.len() == 1 {
			Ok(Columns::single_row(results.into_iter().next().unwrap()))
		} else {
			// For multiple results, we need to create proper column
			// structure
			let column_names = &["operation", "namespace", "view", "primary_key"];
			let rows: Vec<Vec<Value>> = results
				.into_iter()
				.map(|row| row.into_iter().map(|(_, value)| value).collect())
				.collect();
			Ok(Columns::from_rows(column_names, &rows))
		}
	}
}
