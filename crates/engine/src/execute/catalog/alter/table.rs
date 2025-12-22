// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{primary_key::PrimaryKeyToCreate, CatalogStore};
use reifydb_core::{interface::SourceId, value::column::Columns};
use reifydb_rql::plan::{logical::alter::AlterTableOperation, physical::AlterTableNode};
use reifydb_type::{
	diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		query::column_not_found,
	}, return_error,
	Fragment,
	Value,
};

use crate::{execute::Executor, StandardCommandTransaction};

impl Executor {
	pub(crate) async fn alter_table<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: AlterTableNode,
	) -> crate::Result<Columns> {
		// Get namespace and table names from MaybeQualified type
		let namespace_name = plan.node.table.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let table_name = plan.node.table.name.text();

		// Find the namespace
		let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name).await? else {
			let ns_fragment = plan
				.node
				.table
				.namespace
				.clone()
				.unwrap_or_else(|| Fragment::internal("default".to_string()));
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		// Find the table
		let Some(table) = CatalogStore::find_table_by_name(txn, namespace.id, table_name).await? else {
			return_error!(table_not_found(
				plan.node.table.name.clone().into_owned(),
				&namespace.name,
				table_name,
			));
		};

		let mut results = Vec::new();

		// Process each operation
		for operation in plan.node.operations {
			match operation {
				AlterTableOperation::CreatePrimaryKey {
					name,
					columns,
				} => {
					// Get all columns for the table to
					// validate and resolve column IDs
					let table_columns = CatalogStore::list_columns(txn, table.id).await?;

					let mut column_ids = Vec::new();
					for alter_column in columns {
						let column_name = alter_column.column.name.text();

						// Find the column by name
						let Some(column) =
							table_columns.iter().find(|col| col.name == column_name)
						else {
							return_error!(column_not_found(
								alter_column.column.name.clone().into_owned()
							));
						};

						column_ids.push(column.id);
					}

					CatalogStore::create_primary_key(
						txn,
						PrimaryKeyToCreate {
							source: SourceId::Table(table.id),
							column_ids,
						},
					)
					.await?;

					let pk_name = name
						.map(|n| n.text().to_string())
						.unwrap_or_else(|| "unnamed".to_string());

					results.push([
						("operation", Value::Utf8("CREATE PRIMARY KEY".to_string())),
						("namespace", Value::Utf8(namespace.name.clone())),
						("table", Value::Utf8(table.name.clone())),
						("primary_key", Value::Utf8(pk_name)),
					]);
				}
				AlterTableOperation::DropPrimaryKey => {
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
				("table", Value::Utf8(table.name)),
			]))
		} else if results.len() == 1 {
			Ok(Columns::single_row(results.into_iter().next().unwrap()))
		} else {
			// For multiple results, we need to create proper column
			// structure
			let column_names = &["operation", "namespace", "table", "primary_key"];
			let rows: Vec<Vec<Value>> = results
				.into_iter()
				.map(|row| row.into_iter().map(|(_, value)| value).collect())
				.collect();
			Ok(Columns::from_rows(column_names, &rows))
		}
	}
}

// TODO: Add comprehensive tests once Token::testing is properly available
