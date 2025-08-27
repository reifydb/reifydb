// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogTableCommandOperations, CatalogTableQueryOperations, table::TableToCreate};
use reifydb_core::{Value, interface::Transaction};
use reifydb_rql::plan::physical::CreateTablePlan;

use crate::{StandardCommandTransaction, columnar::Columns, execute::Executor};

impl Executor {
	pub(crate) fn create_table<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateTablePlan,
	) -> crate::Result<Columns> {
		// Check if table already exists using the transaction's catalog
		// operations
		if let Some(_) =
			txn.find_table_by_name(plan.schema.id, &plan.table)?
		{
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					(
						"schema",
						Value::Utf8(
							plan.schema
								.name
								.to_string(),
						),
					),
					(
						"table",
						Value::Utf8(
							plan.table.to_string(),
						),
					),
					("created", Value::Bool(false)),
				]));
			}
			// The error will be returned by create_table if the
			// table exists
		}

		txn.create_table(TableToCreate {
			fragment: Some(plan.table.clone()),
			table: plan.table.to_string(),
			schema: plan.schema.id,
			columns: plan.columns,
		})?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(plan.schema.name.to_string())),
			("table", Value::Utf8(plan.table.to_string())),
			("created", Value::Bool(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::test_utils::{create_schema, ensure_test_schema};
	use reifydb_core::{
		OwnedFragment, Value,
		interface::{Params, SchemaDef, SchemaId},
	};
	use reifydb_rql::plan::physical::PhysicalPlan;

	use crate::{
		execute::{Executor, catalog::create::table::CreateTablePlan},
		test_utils::create_test_command_transaction,
	};

	#[test]
	fn test_create_table() {
		let mut txn = create_test_command_transaction();

		let schema = ensure_test_schema(&mut txn);

		let mut plan = CreateTablePlan {
			schema: SchemaDef {
				id: schema.id,
				name: schema.name.clone(),
			},
			table: OwnedFragment::testing("test_table"),
			if_not_exists: false,
			columns: vec![],
		};

		// First creation should succeed
		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateTable(plan.clone()),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("test_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("test_table".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(true));

		// Creating the same table again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateTable(plan.clone()),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("test_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("test_table".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(false));

		// Creating the same table again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateTable(plan),
				Params::default(),
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_table_in_different_schema() {
		let mut txn = create_test_command_transaction();

		let schema = ensure_test_schema(&mut txn);
		let another_schema = create_schema(&mut txn, "another_schema");

		let plan = CreateTablePlan {
			schema: SchemaDef {
				id: schema.id,
				name: schema.name.clone(),
			},
			table: OwnedFragment::testing("test_table"),
			if_not_exists: false,
			columns: vec![],
		};

		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateTable(plan.clone()),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("test_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("test_table".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(true));
		let plan = CreateTablePlan {
			schema: SchemaDef {
				id: another_schema.id,
				name: another_schema.name.clone(),
			},
			table: OwnedFragment::testing("test_table"),
			if_not_exists: false,
			columns: vec![],
		};

		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateTable(plan.clone()),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("another_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("test_table".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(true));
	}

	#[test]
	fn test_create_table_missing_schema() {
		let mut txn = create_test_command_transaction();

		let plan = CreateTablePlan {
			schema: SchemaDef {
				id: SchemaId(999),
				name: "missing_schema".to_string(),
			},
			table: OwnedFragment::testing("my_table"),
			if_not_exists: false,
			columns: vec![],
		};

		// With defensive fallback, this now succeeds even with
		// non-existent schema The table is created with the provided
		// schema ID
		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateTable(plan),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("missing_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("my_table".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(true));
	}
}
