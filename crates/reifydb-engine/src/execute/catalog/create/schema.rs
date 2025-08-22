// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{schema::SchemaToCreate, Catalog};
use reifydb_core::interface::CommandTransaction;
use reifydb_core::{
	interface::Transaction, result::error::diagnostic::catalog::schema_already_exists,
	return_error,
	Value,
};
use reifydb_rql::plan::physical::CreateSchemaPlan;

use crate::{columnar::Columns, execute::Executor};

impl<T: Transaction> Executor<T> {
	pub(crate) fn create_schema(
		&self,
		txn: &mut impl CommandTransaction,
		plan: CreateSchemaPlan,
	) -> crate::Result<Columns> {
		let catalog = Catalog::new();
		if let Some(schema) =
			catalog.find_schema_by_name(txn, &plan.schema)?
		{
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					(
						"schema",
						Value::Utf8(
							plan.schema.to_string(),
						),
					),
					("created", Value::Bool(false)),
				]));
			}

			return_error!(schema_already_exists(
				Some(plan.schema),
				&schema.name,
			));
		}

		catalog.create_schema(
			txn,
			SchemaToCreate {
				schema_fragment: Some(plan.schema.clone()),
				name: plan.schema.to_string(),
			},
		)?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(plan.schema.to_string())),
			("created", Value::Bool(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{interface::Params, OwnedFragment, Value};
	use reifydb_rql::plan::physical::{CreateSchemaPlan, PhysicalPlan};
	use reifydb_transaction::test_utils::create_test_command_transaction;

	use crate::execute::Executor;

	#[test]
	fn test_create_schema() {
		let mut txn = create_test_command_transaction();

		let mut plan = CreateSchemaPlan {
			schema: OwnedFragment::testing("my_schema"),
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
		assert_eq!(result.row(0)[1], Value::Bool(true));

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
		assert_eq!(result.row(0)[1], Value::Bool(false));

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
