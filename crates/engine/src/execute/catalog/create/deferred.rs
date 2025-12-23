// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogViewCommandOperations, CatalogViewQueryOperations, view::ViewToCreate};
use reifydb_core::value::column::Columns;
use reifydb_rql::plan::physical::CreateDeferredViewNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) async fn create_deferred_view<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateDeferredViewNode,
	) -> crate::Result<Columns> {
		if let Some(_) = txn.find_view_by_name(plan.namespace.id, plan.view.text()).await? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("view", Value::Utf8(plan.view.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
		}

		let result = txn
			.create_view(ViewToCreate {
				fragment: Some(plan.view.clone()),
				name: plan.view.text().to_string(),
				namespace: plan.namespace.id,
				columns: plan.columns,
			})
			.await?;

		self.create_deferred_view_flow(txn, &result, plan.as_clause).await?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("view", Value::Utf8(plan.view.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use PhysicalPlan::InlineData;
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::{NamespaceDef, NamespaceId, Params};
	use reifydb_rql::plan::physical::{CreateDeferredViewNode, InlineDataNode, PhysicalPlan};
	use reifydb_type::{Fragment, Value};

	use crate::{
		execute::Executor, stack::Stack, test_utils::create_test_command_transaction_with_internal_schema,
	};

	#[tokio::test]
	async fn test_create_view() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction_with_internal_schema().await;

		let namespace = ensure_test_namespace(&mut txn).await;

		let mut plan = CreateDeferredViewNode {
			namespace: NamespaceDef {
				id: namespace.id,
				name: namespace.name.clone(),
			},
			view: Fragment::internal("test_view"),
			if_not_exists: false,
			columns: vec![],
			as_clause: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
			primary_key: None,
		};

		// First creation should succeed
		let mut stack = Stack::new();
		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.await
			.unwrap()
			.unwrap();

		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

		// Creating the same view again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.await
			.unwrap()
			.unwrap();

		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(false));

		// Creating the same view again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan),
				Params::default(),
				&mut stack,
			)
			.await
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[tokio::test]
	async fn test_create_same_view_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction_with_internal_schema().await;

		let namespace = ensure_test_namespace(&mut txn).await;
		let another_schema = create_namespace(&mut txn, "another_schema").await;

		let plan = CreateDeferredViewNode {
			namespace: NamespaceDef {
				id: namespace.id,
				name: namespace.name.clone(),
			},
			view: Fragment::internal("test_view"),
			if_not_exists: false,
			columns: vec![],
			as_clause: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
			primary_key: None,
		};

		let mut stack = Stack::new();
		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.await
			.unwrap()
			.unwrap();

		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
		let plan = CreateDeferredViewNode {
			namespace: NamespaceDef {
				id: another_schema.id,
				name: another_schema.name.clone(),
			},
			view: Fragment::internal("test_view"),
			if_not_exists: false,
			columns: vec![],
			as_clause: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
			primary_key: None,
		};

		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}

	#[tokio::test]
	async fn test_create_view_missing_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction_with_internal_schema().await;

		let plan = CreateDeferredViewNode {
			namespace: NamespaceDef {
				id: NamespaceId(999),
				name: "missing_schema".to_string(),
			},
			view: Fragment::internal("my_view"),
			if_not_exists: false,
			columns: vec![],
			as_clause: Box::new(InlineData(InlineDataNode {
				rows: vec![],
			})),
			primary_key: None,
		};

		let mut stack = Stack::new();
		instance.execute_command_plan(
			&mut txn,
			PhysicalPlan::CreateDeferredView(plan),
			Params::default(),
			&mut stack,
		)
		.await
		.unwrap_err();
	}
}
