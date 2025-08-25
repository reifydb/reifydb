// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogViewOperations, view::ViewToCreate};
use reifydb_core::{Value, interface::Transaction};
use reifydb_rql::plan::physical::CreateDeferredViewPlan;

use crate::{StandardCommandTransaction, columnar::Columns, execute::Executor};

impl Executor {
	pub(crate) fn create_deferred_view<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateDeferredViewPlan,
	) -> crate::Result<Columns> {
		if let Some(_) =
			txn.find_view_by_name(plan.schema.id, &plan.view)?
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
						"view",
						Value::Utf8(
							plan.view.to_string(),
						),
					),
					("created", Value::Bool(false)),
				]));
			}
		}

		let result = txn.create_view(ViewToCreate {
			fragment: Some(plan.view.clone()),
			name: plan.view.to_string(),
			schema: plan.schema.id,
			columns: plan.columns,
		})?;

		self.create_flow(txn, &result, plan.with)?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(plan.schema.name.to_string())),
			("view", Value::Utf8(plan.view.to_string())),
			("created", Value::Bool(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use PhysicalPlan::InlineData;
	use reifydb_catalog::test_utils::{create_schema, ensure_test_schema};
	use reifydb_core::{
		OwnedFragment, Value,
		interface::{Params, SchemaDef, SchemaId},
	};
	use reifydb_rql::plan::physical::{
		CreateDeferredViewPlan, InlineDataNode, PhysicalPlan,
	};

	use crate::{
		execute::Executor,
		test_utils::create_test_command_transaction_with_internal_schema,
	};

	#[test]
	fn test_create_view() {
		let mut txn =
			create_test_command_transaction_with_internal_schema();

		let schema = ensure_test_schema(&mut txn);

		let mut plan = CreateDeferredViewPlan {
			schema: SchemaDef {
				id: schema.id,
				name: schema.name.clone(),
			},
			view: OwnedFragment::testing("test_view"),
			if_not_exists: false,
			columns: vec![],
			with: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
		};

		// First creation should succeed
		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
			)
			.unwrap();

		assert_eq!(
			result.row(0)[0],
			Value::Utf8("test_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("test_view".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(true));

		// Creating the same view again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
			)
			.unwrap();

		assert_eq!(
			result.row(0)[0],
			Value::Utf8("test_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("test_view".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(false));

		// Creating the same view again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan),
				Params::default(),
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_view_in_different_schema() {
		let mut txn =
			create_test_command_transaction_with_internal_schema();

		let schema = ensure_test_schema(&mut txn);
		let another_schema = create_schema(&mut txn, "another_schema");

		let plan = CreateDeferredViewPlan {
			schema: SchemaDef {
				id: schema.id,
				name: schema.name.clone(),
			},
			view: OwnedFragment::testing("test_view"),
			if_not_exists: false,
			columns: vec![],
			with: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
		};

		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("test_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("test_view".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(true));
		let plan = CreateDeferredViewPlan {
			schema: SchemaDef {
				id: another_schema.id,
				name: another_schema.name.clone(),
			},
			view: OwnedFragment::testing("test_view"),
			if_not_exists: false,
			columns: vec![],
			with: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
		};

		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
			)
			.unwrap();
		assert_eq!(
			result.row(0)[0],
			Value::Utf8("another_schema".to_string())
		);
		assert_eq!(
			result.row(0)[1],
			Value::Utf8("test_view".to_string())
		);
		assert_eq!(result.row(0)[2], Value::Bool(true));
	}

	#[test]
	fn test_create_view_missing_schema() {
		let mut txn =
			create_test_command_transaction_with_internal_schema();

		let plan = CreateDeferredViewPlan {
			schema: SchemaDef {
				id: SchemaId(999),
				name: "missing_schema".to_string(),
			},
			view: OwnedFragment::testing("my_view"),
			if_not_exists: false,
			columns: vec![],
			with: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
		};

		Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan),
				Params::default(),
			)
			.unwrap_err();
	}
}
