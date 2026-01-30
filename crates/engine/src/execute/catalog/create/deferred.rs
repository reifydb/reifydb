// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::view::ViewToCreate;
use reifydb_core::{interface::catalog::change::CatalogTrackViewChangeOperations, value::column::columns::Columns};
use reifydb_rql::plan::physical::CreateDeferredViewNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::execute::Executor;

impl Executor {
	pub(crate) fn create_deferred_view<'a>(
		&self,
		txn: &mut AdminTransaction,
		plan: CreateDeferredViewNode,
	) -> crate::Result<Columns> {
		if let Some(_) = self.catalog.find_view_by_name(txn, plan.namespace.id, plan.view.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("view", Value::Utf8(plan.view.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
		}

		let result = self.catalog.create_deferred_view(
			txn,
			ViewToCreate {
				fragment: Some(plan.view.clone()),
				name: plan.view.text().to_string(),
				namespace: plan.namespace.id,
				columns: plan.columns,
			},
		)?;
		txn.track_view_def_created(result.clone())?;

		self.create_deferred_view_flow(txn, &result, plan.as_clause)?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("view", Value::Utf8(plan.view.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}

#[cfg(test)]
pub mod tests {
	use PhysicalPlan::InlineData;
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::catalog::{id::NamespaceId, namespace::NamespaceDef};
	use reifydb_rql::plan::physical::{CreateDeferredViewNode, InlineDataNode, PhysicalPlan};
	use reifydb_type::{fragment::Fragment, params::Params, value::Value};

	use crate::{execute::Executor, stack::Stack, test_utils::create_test_admin_transaction_with_internal_schema};

	#[test]
	fn test_create_view() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_schema();

		let namespace = ensure_test_namespace(&mut txn);

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
			.execute_admin_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();

		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

		// Creating the same view again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = instance
			.execute_admin_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();

		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(false));

		// Creating the same view again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = instance
			.execute_admin_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan),
				Params::default(),
				&mut stack,
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_view_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_schema();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

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
			.execute_admin_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
				&mut stack,
			)
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
			.execute_admin_plan(
				&mut txn,
				PhysicalPlan::CreateDeferredView(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_view".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}

	#[test]
	fn test_create_view_missing_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_schema();

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
		instance.execute_admin_plan(
			&mut txn,
			PhysicalPlan::CreateDeferredView(plan),
			Params::default(),
			&mut stack,
		)
		.unwrap_err();
	}
}
