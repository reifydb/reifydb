// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::{catalog::table_not_found, query::column_not_found},
	interface::catalog::{change::CatalogTrackTableChangeOperations, primitive::PrimitiveId, table::TableDef},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreatePolicyNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{return_error, value::Value};

use crate::vm::services::Services;

pub(crate) fn create_policy(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreatePolicyNode,
) -> crate::Result<Columns> {
	let namespace_id = plan.namespace.def().id;
	let table_name = plan.table.text();

	// Find the table
	let Some(table) =
		services.catalog.find_table_by_name(&mut Transaction::Admin(txn), namespace_id, table_name)?
	else {
		return_error!(table_not_found(plan.table.clone(), plan.namespace.name(), table_name));
	};

	// Save pre-state for materialized catalog update
	let pre_table = table.clone();

	// Find the column
	let column_name = plan.column.text();
	let Some(column) = services.catalog.find_column_by_name(
		&mut Transaction::Admin(txn),
		PrimitiveId::Table(table.id),
		column_name,
	)?
	else {
		return_error!(column_not_found(plan.column.clone()));
	};

	// Apply each policy to the column
	for policy in &plan.policies {
		services.catalog.create_column_policy(txn, column.id, policy.clone())?;
	}

	// Re-read columns from the KV store to get updated policies, then track the
	// table_def change so the MaterializedCatalogInterceptor refreshes its cache.
	let updated_columns = services.catalog.list_columns(&mut Transaction::Admin(txn), pre_table.id)?;
	let post_table = TableDef {
		columns: updated_columns,
		..pre_table.clone()
	};
	txn.track_table_def_updated(pre_table, post_table)?;

	Ok(Columns::single_row([
		("operation", Value::Utf8("CREATE POLICY".to_string())),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("table", Value::Utf8(table.name)),
		("column", Value::Utf8(column.name)),
	]))
}
