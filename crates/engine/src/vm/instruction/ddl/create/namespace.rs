// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::namespace::NamespaceToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackNamespaceChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateNamespaceNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_namespace(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateNamespaceNode,
) -> crate::Result<Columns> {
	use reifydb_core::interface::catalog::id::NamespaceId;

	let full_name: String = plan.segments.iter().map(|s| s.text()).collect::<Vec<_>>().join(".");

	// Auto-create parent namespaces (mkdir -p semantics)
	let mut parent_id = NamespaceId::ROOT;
	for i in 0..plan.segments.len().saturating_sub(1) {
		let prefix: String = plan.segments[..=i].iter().map(|s| s.text()).collect::<Vec<_>>().join(".");
		if let Some(existing) =
			services.catalog.find_namespace_by_name(&mut Transaction::Admin(txn), &prefix)?
		{
			parent_id = existing.id;
		} else {
			let result = services.catalog.create_namespace(
				txn,
				NamespaceToCreate {
					namespace_fragment: Some(plan.segments[i].clone()),
					name: prefix,
					parent_id,
				},
			)?;
			txn.track_namespace_def_created(result.clone())?;
			parent_id = result.id;
		}
	}

	// Create the final (leaf) namespace
	if let Some(_) = services.catalog.find_namespace_by_name(&mut Transaction::Admin(txn), &full_name)? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(full_name)),
				("created", Value::Boolean(false)),
			]));
		}
	}

	let result = services.catalog.create_namespace(
		txn,
		NamespaceToCreate {
			namespace_fragment: plan.segments.last().cloned(),
			name: full_name,
			parent_id,
		},
	)?;
	txn.track_namespace_def_created(result.clone())?;

	Ok(Columns::single_row([("namespace", Value::Utf8(result.name)), ("created", Value::Boolean(true))]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::auth::Identity;
	use reifydb_type::{params::Params, value::Value};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_namespace() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		// First creation should succeed
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE NAMESPACE my_schema",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("my_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Boolean(true));

		// Creating the same namespace again with `IF NOT EXISTS`
		// should not error
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE NAMESPACE IF NOT EXISTS my_schema",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("my_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Boolean(false));

		// Creating the same namespace again without `IF NOT EXISTS`
		// should return error
		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE NAMESPACE my_schema",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_001");
	}
}
