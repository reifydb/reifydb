// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::namespace::NamespaceToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackNamespaceChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::plan::physical::CreateNamespaceNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_namespace(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateNamespaceNode,
) -> crate::Result<Columns> {
	// Check if namespace already exists using the catalog
	if let Some(_) = services.catalog.find_namespace_by_name(txn, plan.namespace.text())? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
		// The error will be returned by create_namespace if the
		// namespace exists
	}

	let result = services.catalog.create_namespace(
		txn,
		NamespaceToCreate {
			namespace_fragment: Some(plan.namespace.clone()),
			name: plan.namespace.text().to_string(),
		},
	)?;
	txn.track_namespace_def_created(result.clone())?;

	Ok(Columns::single_row([("namespace", Value::Utf8(result.name)), ("created", Value::Boolean(true))]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_rql::plan::physical::{CreateNamespaceNode, PhysicalPlan};
	use reifydb_type::{fragment::Fragment, params::Params, value::Value};

	use crate::{test_utils::create_test_admin_transaction, vm::executor::Executor};

	#[test]
	fn test_create_namespace() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let mut plan = CreateNamespaceNode {
			namespace: Fragment::internal("my_schema"),
			if_not_exists: false,
		};

		// First creation should succeed
		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateNamespace(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("my_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Boolean(true));

		// Creating the same namespace again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateNamespace(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("my_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Boolean(false));

		// Creating the same namespace again with `if_not_exists =
		// false` should return error
		plan.if_not_exists = false;
		let err = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateNamespace(plan), Params::default())
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_001");
	}
}
