// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::{catalog::table_not_found, query::column_not_found},
	interface::catalog::{
		change::{CatalogTrackSeriesChangeOperations, CatalogTrackTableChangeOperations},
		primitive::PrimitiveId,
		table::TableDef,
	},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateColumnPropertyNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{return_error, value::Value};

use crate::{Result, vm::services::Services};

pub(crate) fn create_column_property(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateColumnPropertyNode,
) -> Result<Columns> {
	let namespace_id = plan.namespace.def().id;
	let table_name = plan.table.text();

	// Find the table
	let table = services.catalog.find_table_by_name(&mut Transaction::Admin(txn), namespace_id, table_name)?;

	if let Some(table) = table {
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

		// Apply each property to the column
		for property in &plan.properties {
			services.catalog.create_column_property(txn, column.id, property.clone())?;
		}

		// Re-read columns from the KV store to get updated properties, then track the
		// table_def change so the MaterializedCatalogInterceptor refreshes its cache.
		let updated_columns = services.catalog.list_columns(&mut Transaction::Admin(txn), pre_table.id)?;
		let post_table = TableDef {
			columns: updated_columns,
			..pre_table.clone()
		};
		txn.track_table_def_updated(pre_table, post_table)?;

		Ok(Columns::single_row([
			("operation", Value::Utf8("CREATE COLUMN PROPERTY".to_string())),
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("table", Value::Utf8(table.name)),
			("column", Value::Utf8(column.name)),
		]))
	} else {
		// Try series
		let Some(series) =
			services.catalog.find_series_by_name(&mut Transaction::Admin(txn), namespace_id, table_name)?
		else {
			return_error!(table_not_found(plan.table.clone(), plan.namespace.name(), table_name));
		};

		let pre_series = series.clone();

		let column_name = plan.column.text();
		let Some(column) = services.catalog.find_column_by_name(
			&mut Transaction::Admin(txn),
			PrimitiveId::Series(series.id),
			column_name,
		)?
		else {
			return_error!(column_not_found(plan.column.clone()));
		};

		for property in &plan.properties {
			services.catalog.create_column_property(txn, column.id, property.clone())?;
		}

		// Re-read series def to get updated column properties
		let post_series = services.catalog.get_series(&mut Transaction::Admin(txn), series.id)?;
		txn.track_series_def_updated(pre_series, post_series)?;

		Ok(Columns::single_row([
			("operation", Value::Utf8("CREATE COLUMN PROPERTY".to_string())),
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("series", Value::Utf8(series.name)),
			("column", Value::Utf8(column.name)),
		]))
	}
}
