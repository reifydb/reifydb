// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogStore, schema::SchemaToCreate};
use reifydb_core::{
	Value, interface::Transaction,
	result::error::diagnostic::catalog::schema_already_exists,
	return_error,
};
use reifydb_rql::plan::physical::CreateSchemaPlan;

use crate::{
	StandardCommandTransaction, columnar::Columns, execute::Executor,
	transaction::operation::SchemaDefCreateOperation,
};

impl Executor {
	pub(crate) fn create_schema<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateSchemaPlan,
	) -> crate::Result<Columns> {
		if let Some(schema) =
			CatalogStore::find_schema_by_name(txn, &plan.schema)?
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

		let result = txn.create_schema_def(SchemaToCreate {
			schema_fragment: Some(plan.schema.clone()),
			name: plan.schema.to_string(),
		})?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(result.name)),
			("created", Value::Bool(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{OwnedFragment, Value, interface::Params};
	use reifydb_rql::plan::physical::{CreateSchemaPlan, PhysicalPlan};

	use crate::{
		execute::Executor, test_utils::create_test_command_transaction,
	};

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
