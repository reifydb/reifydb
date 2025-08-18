// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{Catalog, view::ViewToCreate};
use reifydb_core::{
	Value,
	interface::{
		ActiveCommandTransaction, Command, ExecuteCommand, Identity,
		Params, Transaction, ViewDef,
	},
	result::error::diagnostic::catalog::view_already_exists,
	return_error,
};
use reifydb_flow::compile_flow;
use reifydb_rql::plan::physical::{CreateComputedViewPlan, PhysicalPlan};

use crate::{columnar::Columns, execute::Executor};

impl<T: Transaction> Executor<T> {
	pub(crate) fn create_computed_view(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		plan: CreateComputedViewPlan,
	) -> crate::Result<Columns> {
		if let Some(view) = Catalog::get_view_by_name(
			txn,
			plan.schema.id,
			&plan.view,
		)? {
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

			return_error!(view_already_exists(
				Some(plan.view.clone()),
				&plan.schema.name,
				&view.name,
			));
		}

		let result = Catalog::create_view(
			txn,
			ViewToCreate {
				fragment: Some(plan.view.clone()),
				view: plan.view.to_string(),
				schema: plan.schema.name.to_string(),
				columns: plan.columns,
			},
		)?;

		self.create_flow(txn, &result, plan.with)?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(plan.schema.name.to_string())),
			("view", Value::Utf8(plan.view.to_string())),
			("created", Value::Bool(true)),
		]))
	}

	fn create_flow(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		view: &ViewDef,
		plan: Option<Box<PhysicalPlan>>,
	) -> crate::Result<()> {
		let Some(plan) = plan else {
			return Ok(());
		};

		let flow = compile_flow(txn, *plan, view).unwrap();

		let rql = r#"
                 from[{data: blob::utf8('$REPLACE')}]
                 insert reifydb.flows
             "#
		.replace(
			"$REPLACE",
			serde_json::to_string(&flow).unwrap().as_str(),
		);

		self.execute_command(
			txn,
			Command {
				rql: rql.as_str(),
				params: Params::default(),
				identity: &Identity::root(),
			},
		)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::{
		schema::SchemaDef,
		test_utils::{create_schema, ensure_test_schema},
	};
	use reifydb_core::{
		OwnedFragment, Value,
		interface::{Params, SchemaId},
	};
	use reifydb_rql::plan::physical::{
		CreateComputedViewPlan, PhysicalPlan,
	};
	use reifydb_transaction::test_utils::create_test_command_transaction;

	use crate::execute::Executor;

	#[test]
	fn test_create_view() {
		let mut txn = create_test_command_transaction();

		let schema = ensure_test_schema(&mut txn);

		let mut plan = CreateComputedViewPlan {
			schema: SchemaDef {
				id: schema.id,
				name: schema.name.clone(),
			},
			view: OwnedFragment::testing("test_view"),
			if_not_exists: false,
			columns: vec![],
			with: None,
		};

		// First creation should succeed
		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateComputedView(plan.clone()),
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
				PhysicalPlan::CreateComputedView(plan.clone()),
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
				PhysicalPlan::CreateComputedView(plan),
				Params::default(),
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_view_in_different_schema() {
		let mut txn = create_test_command_transaction();

		let schema = ensure_test_schema(&mut txn);
		let another_schema = create_schema(&mut txn, "another_schema");

		let plan = CreateComputedViewPlan {
			schema: SchemaDef {
				id: schema.id,
				name: schema.name.clone(),
			},
			view: OwnedFragment::testing("test_view"),
			if_not_exists: false,
			columns: vec![],
			with: None,
		};

		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateComputedView(plan.clone()),
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
		let plan = CreateComputedViewPlan {
			schema: SchemaDef {
				id: another_schema.id,
				name: another_schema.name.clone(),
			},
			view: OwnedFragment::testing("test_view"),
			if_not_exists: false,
			columns: vec![],
			with: None,
		};

		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateComputedView(plan.clone()),
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
		let mut txn = create_test_command_transaction();

		let plan = CreateComputedViewPlan {
			schema: SchemaDef {
				id: SchemaId(999),
				name: "missing_schema".to_string(),
			},
			view: OwnedFragment::testing("my_view"),
			if_not_exists: false,
			columns: vec![],
			with: None,
		};

		let err = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateComputedView(plan),
				Params::default(),
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_002");
	}
}
