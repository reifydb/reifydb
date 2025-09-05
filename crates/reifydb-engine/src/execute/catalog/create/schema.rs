// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogSchemaCommandOperations, CatalogSchemaQueryOperations,
	schema::SchemaToCreate,
};
use reifydb_core::{interface::Transaction, value::columnar::Columns};
use reifydb_rql::plan::physical::CreateSchemaPlan;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_schema<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateSchemaPlan,
	) -> crate::Result<Columns> {
		// Check if schema already exists using the transaction's
		// catalog operations
		if let Some(_) = txn.find_schema_by_name(plan.schema.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					(
						"schema",
						Value::Utf8(
							plan.schema
								.text()
								.to_string(),
						),
					),
					("created", Value::Boolean(false)),
				]));
			}
			// The error will be returned by create_schema if the
			// schema exists
		}

		let result = txn.create_schema(SchemaToCreate {
			schema_fragment: Some(plan.schema.clone().into_owned()),
			name: plan.schema.text().to_string(),
		})?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(result.name)),
			("created", Value::Boolean(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::Params;
	use reifydb_rql::plan::physical::{CreateSchemaPlan, PhysicalPlan};
	use reifydb_type::{Fragment, Value};

	use crate::{
		execute::Executor, test_utils::create_test_command_transaction,
	};

	#[test]
	fn test_create_schema() {
		let mut txn = create_test_command_transaction();

		let mut plan = CreateSchemaPlan {
			schema: Fragment::owned_internal("my_schema"),
			if_not_exists: false,
		};

		// First creation should succeed
		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateSchema(plan.clone()),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("my_schema".to_string())
		);
		assert_eq!(result.row(0)[1], Value::Boolean(true));

		// Creating the same schema again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateSchema(plan.clone()),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("my_schema".to_string())
		);
		assert_eq!(result.row(0)[1], Value::Boolean(false));

		// Creating the same schema again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateSchema(plan),
				Params::default(),
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_001");
	}
}
