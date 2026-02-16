// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::{primary_key::PrimaryKeyToCreate, table::TableToCreate};
use reifydb_core::{
	error::diagnostic::query::column_not_found,
	interface::catalog::{change::CatalogTrackTableChangeOperations, primitive::PrimitiveId},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateTableNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{return_error, value::Value};

use crate::vm::services::Services;

pub(crate) fn create_table(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateTableNode,
) -> crate::Result<Columns> {
	// Check if table already exists using the catalog
	if let Some(_) = services.catalog.find_table_by_name(txn, plan.namespace.def().id, plan.table.text())? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("table", Value::Utf8(plan.table.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
		// The error will be returned by create_table if the
		// table exists
	}

	let table = services.catalog.create_table(
		txn,
		TableToCreate {
			name: plan.table.clone(),
			namespace: plan.namespace.def().id,
			columns: plan.columns,
			retention_policy: None,
			primary_key_columns: None,
		},
	)?;
	txn.track_table_def_created(table.clone())?;

	// If primary key is specified, create it immediately
	if let Some(pk_def) = plan.primary_key {
		// Get the table columns to resolve column IDs
		let table_columns = services.catalog.list_columns(txn, table.id)?;

		// Resolve column names to IDs
		let mut column_ids = Vec::new();
		for pk_column in pk_def.columns {
			let column_name = pk_column.column.text();
			let Some(column) = table_columns.iter().find(|col| col.name == column_name) else {
				return_error!(column_not_found(pk_column.column));
			};
			column_ids.push(column.id);
		}

		// Create primary key
		services.catalog.create_primary_key(
			txn,
			PrimaryKeyToCreate {
				source: PrimitiveId::Table(table.id),
				column_ids,
			},
		)?;
	}

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("table", Value::Utf8(plan.table.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::auth::Identity;
	use reifydb_type::{params::Params, value::Value};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_table() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		// Create namespace first
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		// First creation should succeed
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE test_namespace.test_table { id: Int4 }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_table".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Creating the same table again should return error
		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE test_namespace.test_table { id: Int4 }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_table_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		// Create both namespaces
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE another_schema",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		// Create table in first namespace
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE test_namespace.test_table { id: Int4 }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_table".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Create table with same name in different namespace
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE another_schema.test_table { id: Int4 }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_table".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}
}
