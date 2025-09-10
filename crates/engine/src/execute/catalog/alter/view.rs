// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogStore, primary_key::PrimaryKeyToCreate};
use reifydb_core::{
	interface::{SourceId, Transaction},
	return_error,
	value::columnar::Columns,
};
use reifydb_rql::{ast::AstAlterViewOperation, plan::physical::AlterViewPlan};
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn execute_alter_view<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: AlterViewPlan,
	) -> crate::Result<Columns> {
		// Use default schema if not provided
		let schema_name = plan
			.node
			.view
			.view
			.schema
			.as_ref()
			.map(|s| s.text())
			.unwrap_or("public");
		let view_name = plan.node.view.view.name.text();

		// Find the schema
		let Some(schema) =
			CatalogStore::find_schema_by_name(txn, schema_name)?
		else {
			return_error!(reifydb_core::diagnostic::catalog::schema_not_found(
				plan.node.view.view.schema.clone()
					.map(|s| s.into_owned()),
				schema_name,
			));
		};

		// Find the view
		let Some(view) = CatalogStore::find_view_by_name(
			txn, schema.id, view_name,
		)?
		else {
			return_error!(reifydb_core::diagnostic::catalog::view_not_found(
				plan.node.view.view.name.clone().into_owned(),
				&schema.name,
				view_name,
			));
		};

		let mut results = Vec::new();

		// Process each operation
		for operation in plan.node.view.operations {
			match operation {
				AstAlterViewOperation::CreatePrimaryKey {
					name,
					columns,
				} => {
					// Get all columns for the view to
					// validate and resolve column IDs
					let view_columns =
						CatalogStore::list_columns(
							txn, view.id,
						)?;

					// Map column names to IDs
					let mut column_ids = Vec::new();
					for ast_column in columns {
						let column_fragment =
							ast_column
								.column
								.clone()
								.fragment();
						let column_name =
							column_fragment.text();

						// Find the column by name
						let Some(column) = view_columns
							.iter()
							.find(|col| {
								col.name == column_name
							})
						else {
							return_error!(reifydb_core::diagnostic::query::column_not_found(
								ast_column.column.fragment().into_owned()
							));
						};

						column_ids.push(column.id);
					}

					// Create the primary key
					CatalogStore::create_primary_key(
						txn,
						PrimaryKeyToCreate {
							source: SourceId::View(
								view.id,
							),
							column_ids,
						},
					)?;

					let pk_name = name
						.map(|n| {
							n.fragment()
								.text()
								.to_string()
						})
						.unwrap_or_else(|| {
							"unnamed".to_string()
						});

					results.push([
						("operation", Value::Utf8("CREATE PRIMARY KEY".to_string())),
						("schema", Value::Utf8(schema.name.clone())),
						("view", Value::Utf8(view.name.clone())),
						("primary_key", Value::Utf8(pk_name)),
					]);
				}
				AstAlterViewOperation::DropPrimaryKey => {
					// Not implemented per requirements
					continue;
				}
			}
		}

		// Return results for all operations performed
		if results.is_empty() {
			// No operations performed, return empty result
			Ok(Columns::single_row([
				(
					"operation",
					Value::Utf8(
						"NO OPERATIONS".to_string(),
					),
				),
				("schema", Value::Utf8(schema.name)),
				("view", Value::Utf8(view.name)),
			]))
		} else if results.len() == 1 {
			Ok(Columns::single_row(
				results.into_iter().next().unwrap(),
			))
		} else {
			// For multiple results, we need to create proper column
			// structure
			let column_names =
				&["operation", "schema", "view", "primary_key"];
			let rows: Vec<Vec<Value>> = results
				.into_iter()
				.map(|row| {
					row.into_iter()
						.map(|(_, value)| value)
						.collect()
				})
				.collect();
			Ok(Columns::from_rows(column_names, &rows))
		}
	}
}
