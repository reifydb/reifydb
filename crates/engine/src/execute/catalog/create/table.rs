// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogTableCommandOperations, CatalogTableQueryOperations, table::TableToCreate};
use reifydb_core::{interface::Transaction, value::column::Columns};
use reifydb_rql::plan::physical::CreateTableNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_table<'a, T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateTableNode,
	) -> crate::Result<Columns<'a>> {
		// Check if table already exists using the transaction's catalog
		// operations
		if let Some(_) = txn.find_table_by_name(plan.namespace.def().id, plan.table.name.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name().to_string())),
					("table", Value::Utf8(plan.table.name.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
			// The error will be returned by create_table if the
			// table exists
		}

		txn.create_table(TableToCreate {
			fragment: Some(plan.table.name.clone().into_owned()),
			table: plan.table.name.text().to_string(),
			namespace: plan.namespace.def().id,
			columns: plan.columns,
		})?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("table", Value::Utf8(plan.table.name.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::{
		NamespaceDef, NamespaceId, Params, TableIdentifier, identifier::NamespaceIdentifier,
		resolved::ResolvedNamespace,
	};
	use reifydb_rql::plan::physical::PhysicalPlan;
	use reifydb_type::{Fragment, Value};

	use crate::{
		execute::{Executor, catalog::create::table::CreateTableNode},
		test_utils::create_test_command_transaction,
	};

	#[test]
	fn test_create_table() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let namespace_id = NamespaceIdentifier::new(Fragment::owned_internal("test_namespace"));
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace.clone());
		let mut plan = CreateTableNode {
			namespace: resolved_namespace.clone(),
			table: TableIdentifier::new(
				Fragment::owned_internal("test_namespace"),
				Fragment::owned_internal("test_table"),
			),
			if_not_exists: false,
			columns: vec![],
		};

		// First creation should succeed
		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateTable(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

		// Creating the same table again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateTable(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(false));

		// Creating the same table again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateTable(plan), Params::default())
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_table_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

		let namespace_id = NamespaceIdentifier::new(Fragment::owned_internal("test_namespace"));
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace.clone());
		let plan = CreateTableNode {
			namespace: resolved_namespace,
			table: TableIdentifier::new(
				Fragment::owned_internal("test_namespace"),
				Fragment::owned_internal("test_table"),
			),
			if_not_exists: false,
			columns: vec![],
		};

		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateTable(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
		let namespace_id = NamespaceIdentifier::new(Fragment::owned_internal("another_schema"));
		let resolved_namespace = ResolvedNamespace::new(namespace_id, another_schema.clone());
		let plan = CreateTableNode {
			namespace: resolved_namespace,
			table: TableIdentifier::new(
				Fragment::owned_internal("another_schema"),
				Fragment::owned_internal("test_table"),
			),
			if_not_exists: false,
			columns: vec![],
		};

		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateTable(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}

	#[test]
	fn test_create_table_missing_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let namespace_id = NamespaceIdentifier::new(Fragment::owned_internal("missing_schema"));
		let namespace_def = NamespaceDef {
			id: NamespaceId(999),
			name: "missing_schema".to_string(),
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
		let plan = CreateTableNode {
			namespace: resolved_namespace,
			table: TableIdentifier::new(
				Fragment::owned_internal("missing_schema"),
				Fragment::owned_internal("my_table"),
			),
			if_not_exists: false,
			columns: vec![],
		};

		// With defensive fallback, this now succeeds even with
		// non-existent namespace The table is created with the provided
		// namespace ID
		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateTable(plan), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("missing_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("my_table".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}
}
