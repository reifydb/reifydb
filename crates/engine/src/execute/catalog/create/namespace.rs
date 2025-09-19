// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogNamespaceCommandOperations, CatalogNamespaceQueryOperations, namespace::NamespaceToCreate,
};
use reifydb_core::{interface::Transaction, value::columnar::Columns};
use reifydb_rql::plan::physical::CreateNamespaceNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_namespace<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateNamespaceNode,
	) -> crate::Result<Columns> {
		// Check if namespace already exists using the transaction's
		// catalog operations
		if let Some(_) = txn.find_namespace_by_name(plan.namespace.as_borrowed())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
			// The error will be returned by create_namespace if the
			// namespace exists
		}

		let result = txn.create_namespace(NamespaceToCreate {
			namespace_fragment: Some(plan.namespace.clone().into_owned()),
			name: plan.namespace.text().to_string(),
		})?;

		Ok(Columns::single_row([("namespace", Value::Utf8(result.name)), ("created", Value::Boolean(true))]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::Params;
	use reifydb_rql::plan::physical::{CreateNamespaceNode, PhysicalPlan};
	use reifydb_type::{Fragment, Value};

	use crate::{execute::Executor, test_utils::create_test_command_transaction};

	#[test]
	fn test_create_namespace() {
		let mut txn = create_test_command_transaction();

		let mut plan = CreateNamespaceNode {
			namespace: Fragment::owned_internal("my_schema"),
			if_not_exists: false,
		};

		// First creation should succeed
		let result = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateNamespace(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Boolean(true));

		// Creating the same namespace again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateNamespace(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Boolean(false));

		// Creating the same namespace again with `if_not_exists =
		// false` should return error
		plan.if_not_exists = false;
		let err = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateNamespace(plan), Params::default())
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_001");
	}
}
