// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::primary_key::PrimaryKeyToCreate;
use reifydb_core::{
	error::diagnostic::{catalog::table_not_found, query::column_not_found},
	interface::catalog::primitive::PrimitiveId,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreatePrimaryKeyNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{return_error, value::Value};

use crate::{Result, vm::services::Services};

pub(crate) fn create_primary_key(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreatePrimaryKeyNode,
) -> Result<Columns> {
	let namespace_id = plan.namespace.def().id;
	let table_name = plan.table.text();

	// Find the table
	let Some(table) =
		services.catalog.find_table_by_name(&mut Transaction::Admin(txn), namespace_id, table_name)?
	else {
		return_error!(table_not_found(plan.table.clone(), plan.namespace.name(), table_name));
	};

	// Get all columns for the table to validate and resolve column IDs
	let table_columns = services.catalog.list_columns(&mut Transaction::Admin(txn), table.id)?;

	let mut column_ids = Vec::new();
	for pk_column in &plan.columns {
		let column_name = pk_column.column.text();

		// Find the column by name
		let Some(column) = table_columns.iter().find(|col| col.name == column_name) else {
			return_error!(column_not_found(pk_column.column.clone()));
		};

		column_ids.push(column.id);
	}

	services.catalog.create_primary_key(
		txn,
		PrimaryKeyToCreate {
			source: PrimitiveId::Table(table.id),
			column_ids,
		},
	)?;

	Ok(Columns::single_row([
		("operation", Value::Utf8("CREATE PRIMARY KEY".to_string())),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("table", Value::Utf8(table.name)),
	]))
}
