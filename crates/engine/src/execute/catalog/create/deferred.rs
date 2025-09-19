// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogViewCommandOperations, CatalogViewQueryOperations, view::ViewToCreate};
use reifydb_core::{interface::Transaction, value::columnar::Columns};
use reifydb_rql::plan::physical::CreateDeferredViewNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_deferred_view<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateDeferredViewNode,
	) -> crate::Result<Columns> {
		if let Some(_) = txn.find_view_by_name(plan.namespace.id, plan.view.name.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("view", Value::Utf8(plan.view.name.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
		}

		let result = txn.create_view(ViewToCreate {
			fragment: Some(plan.view.name.clone().into_owned()),
			name: plan.view.name.text().to_string(),
			namespace: plan.namespace.id,
			columns: plan.columns,
		})?;

		self.create_flow(txn, &result, plan.with)?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("view", Value::Utf8(plan.view.name.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use PhysicalPlan::InlineData;
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::{DeferredViewIdentifier, NamespaceDef, NamespaceId, Params};
	use reifydb_rql::plan::physical::{CreateDeferredViewNode, InlineDataNode, PhysicalPlan};
	use reifydb_type::{Fragment, Value};

	use crate::{execute::Executor, test_utils::create_test_command_transaction_with_internal_schema};

	#[test]
	fn test_create_view() {
		let mut txn = create_test_command_transaction_with_internal_schema();

		let namespace = ensure_test_namespace(&mut txn);

		let mut plan = CreateDeferredViewNode {
			namespace: NamespaceDef {
				id: namespace.id,
				name: namespace.name.clone(),
			},
			view: DeferredViewIdentifier::new(
				Fragment::owned_internal("test_namespace"),
				Fragment::owned_internal("test_view"),
			),
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

		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

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

		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(false));

		// Creating the same view again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateDeferredView(plan), Params::default())
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_view_in_different_schema() {
		let mut txn = create_test_command_transaction_with_internal_schema();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

		let plan = CreateDeferredViewNode {
			namespace: NamespaceDef {
				id: namespace.id,
				name: namespace.name.clone(),
			},
			view: DeferredViewIdentifier::new(
				Fragment::owned_internal("test_namespace"),
				Fragment::owned_internal("test_view"),
			),
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
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
		let plan = CreateDeferredViewNode {
			namespace: NamespaceDef {
				id: another_schema.id,
				name: another_schema.name.clone(),
			},
			view: DeferredViewIdentifier::new(
				Fragment::owned_internal("test_namespace"),
				Fragment::owned_internal("test_view"),
			),
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
		assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}

	#[test]
	fn test_create_view_missing_schema() {
		let mut txn = create_test_command_transaction_with_internal_schema();

		let plan = CreateDeferredViewNode {
			namespace: NamespaceDef {
				id: NamespaceId(999),
				name: "missing_schema".to_string(),
			},
			view: DeferredViewIdentifier::new(
				Fragment::owned_internal("another_schema"),
				Fragment::owned_internal("my_view"),
			),
			if_not_exists: false,
			columns: vec![],
			with: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
		};

		Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateDeferredView(plan), Params::default())
			.unwrap_err();
	}
}
