// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::view::ViewToCreate;
use reifydb_core::{interface::catalog::change::CatalogTrackViewChangeOperations, value::column::columns::Columns};
use reifydb_rql::plan::physical::CreateDeferredViewNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use super::create_deferred_view_flow;
use crate::vm::services::Services;

pub(crate) fn create_deferred_view(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateDeferredViewNode,
) -> crate::Result<Columns> {
	if let Some(_) = services.catalog.find_view_by_name(txn, plan.namespace.id, plan.view.text())? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name.to_string())),
				("view", Value::Utf8(plan.view.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
	}

	let result = services.catalog.create_deferred_view(
		txn,
		ViewToCreate {
			fragment: Some(plan.view.clone()),
			name: plan.view.text().to_string(),
			namespace: plan.namespace.id,
			columns: plan.columns,
		},
	)?;
	txn.track_view_def_created(result.clone())?;

	create_deferred_view_flow(&services.catalog, txn, &result, plan.as_clause)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name.to_string())),
		("view", Value::Utf8(plan.view.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use PhysicalPlan::InlineData;
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::catalog::{id::NamespaceId, namespace::NamespaceDef};
	use reifydb_rql::plan::physical::{CreateDeferredViewNode, InlineDataNode, PhysicalPlan};
	use reifydb_type::{fragment::Fragment, params::Params, value::Value};

	use crate::{test_utils::create_test_admin_transaction_with_internal_schema, vm::executor::Executor};

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
		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDeferredView(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Creating the same view again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDeferredView(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(false));

		// Creating the same view again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDeferredView(plan), Params::default())
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

		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDeferredView(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
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

		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDeferredView(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
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

		instance.run_admin_plan(&mut txn, PhysicalPlan::CreateDeferredView(plan), Params::default())
			.unwrap_err();
	}
}
