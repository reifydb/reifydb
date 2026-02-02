// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::{primary_key::PrimaryKeyToCreate, table::TableToCreate};
use reifydb_core::{
	error::diagnostic::query::column_not_found,
	interface::catalog::{change::CatalogTrackTableChangeOperations, primitive::PrimitiveId},
	value::column::columns::Columns,
};
use reifydb_rql::plan::physical::CreateTableNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{return_error, value::Value};

use crate::execute::Executor;

impl Executor {
	pub(crate) fn create_table<'a>(
		&self,
		txn: &mut AdminTransaction,
		plan: CreateTableNode,
	) -> crate::Result<Columns> {
		// Check if table already exists using the catalog
		if let Some(_) = self.catalog.find_table_by_name(txn, plan.namespace.def().id, plan.table.text())? {
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

		let table = self.catalog.create_table(
			txn,
			TableToCreate {
				fragment: Some(plan.table.clone()),
				table: plan.table.text().to_string(),
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
			let table_columns = self.catalog.list_columns(txn, table.id)?;

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
			self.catalog.create_primary_key(
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
}

#[cfg(test)]
pub mod tests {
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::{
		catalog::{id::NamespaceId, namespace::NamespaceDef},
		resolved::ResolvedNamespace,
	};
	use reifydb_rql::plan::physical::PhysicalPlan;
	use reifydb_type::{fragment::Fragment, params::Params, value::Value};

	use crate::{
		execute::{Executor, ddl::create::table::CreateTableNode},
		stack::Stack,
		test_utils::create_test_admin_transaction,
	};

	#[test]
	fn test_create_table() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let namespace_ident = Fragment::internal("test_namespace");
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
		let mut plan = CreateTableNode {
			namespace: resolved_namespace.clone(),
			table: Fragment::internal("test_table"),
			if_not_exists: false,
			columns: vec![],
			primary_key: None,
		};

		// First creation should succeed
		let mut stack = Stack::new();
		let result = instance
			.dispatch_admin(
				&mut txn,
				PhysicalPlan::CreateTable(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

		// Creating the same table again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = instance
			.dispatch_admin(
				&mut txn,
				PhysicalPlan::CreateTable(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(false));

		// Creating the same table again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = instance
			.dispatch_admin(&mut txn, PhysicalPlan::CreateTable(plan), Params::default(), &mut stack)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_table_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

		let namespace_ident = Fragment::internal("test_namespace");
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
		let plan = CreateTableNode {
			namespace: resolved_namespace,
			table: Fragment::internal("test_table"),
			if_not_exists: false,
			columns: vec![],
			primary_key: None,
		};

		let mut stack = Stack::new();
		let result = instance
			.dispatch_admin(
				&mut txn,
				PhysicalPlan::CreateTable(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
		let namespace_ident = Fragment::internal("another_schema");
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, another_schema.clone());
		let plan = CreateTableNode {
			namespace: resolved_namespace,
			table: Fragment::internal("test_table"),
			if_not_exists: false,
			columns: vec![],
			primary_key: None,
		};

		let result = instance
			.dispatch_admin(
				&mut txn,
				PhysicalPlan::CreateTable(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}

	#[test]
	fn test_create_table_missing_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let namespace_ident = Fragment::internal("missing_schema");
		let namespace_def = NamespaceDef {
			id: NamespaceId(999),
			name: "missing_schema".to_string(),
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace_def);
		let plan = CreateTableNode {
			namespace: resolved_namespace,
			table: Fragment::internal("my_table"),
			if_not_exists: false,
			columns: vec![],
			primary_key: None,
		};

		let mut stack = Stack::new();
		let result = instance
			.dispatch_admin(&mut txn, PhysicalPlan::CreateTable(plan), Params::default(), &mut stack)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("missing_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("my_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}
}
