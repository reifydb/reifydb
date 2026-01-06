// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{CatalogStore, namespace::NamespaceToCreate};
use reifydb_core::{interface::CatalogTrackNamespaceChangeOperations, value::column::Columns};
use reifydb_rql::plan::physical::CreateNamespaceNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_namespace<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateNamespaceNode,
	) -> crate::Result<Columns> {
		// Check if namespace already exists using the catalog
		if let Some(_) = self.catalog.find_namespace_by_name(txn, plan.namespace.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
			// The error will be returned by create_namespace if the
			// namespace exists
		}

		let result = CatalogStore::create_namespace(
			txn,
			NamespaceToCreate {
				namespace_fragment: Some(plan.namespace.clone()),
				name: plan.namespace.text().to_string(),
			},
		)?;
		txn.track_namespace_def_created(result.clone())?;

		Ok(Columns::single_row([("namespace", Value::Utf8(result.name)), ("created", Value::Boolean(true))]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::Params;
	use reifydb_rql::plan::physical::{CreateNamespaceNode, PhysicalPlan};
	use reifydb_type::{Fragment, Value};

	use crate::{execute::Executor, stack::Stack, test_utils::create_test_command_transaction};

	#[test]
	fn test_create_namespace() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let mut plan = CreateNamespaceNode {
			namespace: Fragment::internal("my_schema"),
			if_not_exists: false,
		};

		// First creation should succeed
		let mut stack = Stack::new();
		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateNamespace(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();

		assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Boolean(true));

		// Creating the same namespace again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateNamespace(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Boolean(false));

		// Creating the same namespace again with `if_not_exists =
		// false` should return error
		plan.if_not_exists = false;
		let err = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateNamespace(plan),
				Params::default(),
				&mut stack,
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_001");
	}
}
