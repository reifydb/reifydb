// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::namespace::NamespaceToCreate;
use reifydb_core::{
	interface::catalog::{change::CatalogTrackNamespaceChangeOperations, id::NamespaceId},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateRemoteNamespaceNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_remote_namespace(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateRemoteNamespaceNode,
) -> Result<Columns> {
	let full_name: String = plan.segments.iter().map(|s| s.text()).collect::<Vec<_>>().join("::");

	// Auto-create parent namespaces (mkdir -p semantics)
	let mut parent_id = NamespaceId::ROOT;
	for i in 0..plan.segments.len().saturating_sub(1) {
		let prefix: String = plan.segments[..=i].iter().map(|s| s.text()).collect::<Vec<_>>().join("::");
		if let Some(existing) =
			services.catalog.find_namespace_by_name(&mut Transaction::Admin(txn), &prefix)?
		{
			parent_id = existing.id();
		} else {
			let result = services.catalog.create_namespace(
				txn,
				NamespaceToCreate {
					namespace_fragment: Some(plan.segments[i].clone()),
					name: prefix,
					local_name: plan.segments[i].text().to_string(),
					parent_id,
					grpc: None,
				},
			)?;
			txn.track_namespace_created(result.clone())?;
			parent_id = result.id();
		}
	}

	// Create the final (leaf) namespace with grpc
	if let Some(_) = services.catalog.find_namespace_by_name(&mut Transaction::Admin(txn), &full_name)? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(full_name)),
				("created", Value::Boolean(false)),
			]));
		}
	}

	let grpc_text = plan.grpc.text().to_string();
	let result = services.catalog.create_namespace(
		txn,
		NamespaceToCreate {
			namespace_fragment: plan.segments.last().cloned(),
			name: full_name,
			local_name: plan.segments.last().unwrap().text().to_string(),
			parent_id,
			grpc: Some(grpc_text),
		},
	)?;
	txn.track_namespace_created(result.clone())?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(result.name().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_transaction::transaction::query::QueryTransaction;
	use reifydb_type::{
		params::Params,
		value::{Value, identity::IdentityId},
	};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{Admin, Query, executor::Executor},
	};

	#[test]
	fn test_create_remote_namespace() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE REMOTE NAMESPACE remote_ns WITH { grpc: 'localhost:50051' }",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("remote_ns".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Boolean(true));
	}

	#[test]
	fn test_create_remote_namespace_if_not_exists() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		// First creation
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE REMOTE NAMESPACE remote_ns WITH { grpc: 'localhost:50051' }",
				params: Params::default(),
			},
		)
		.unwrap();

		// Second creation with IF NOT EXISTS should not error
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE REMOTE NAMESPACE IF NOT EXISTS remote_ns WITH { grpc: 'localhost:50051' }",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("remote_ns".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Boolean(false));
	}

	#[test]
	fn test_query_remote_namespace_without_registry_returns_empty() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = IdentityId::root();

		// Create a remote namespace
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE REMOTE NAMESPACE remote_ns WITH { grpc: 'http://localhost:50051' }",
				params: Params::default(),
			},
		)
		.unwrap();
		txn.commit().unwrap();

		// Query compiles to RemoteScan; without a RemoteRegistry it returns empty frames
		let mut qt =
			QueryTransaction::new(txn.multi.clone().begin_query().unwrap(), txn.single.clone(), identity);
		let frames = instance
			.query(
				&mut qt,
				Query {
					rql: "FROM remote_ns::some_table",
					params: Params::default(),
				},
			)
			.unwrap();

		// Without a remote registry, the RemoteFetchNode produces no data
		assert!(frames.is_empty() || frames.iter().all(|f| f.columns.is_empty()));
	}

	#[test]
	fn test_create_remote_namespace_nested() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE REMOTE NAMESPACE blockchain::protocol WITH { grpc: '10.0.0.5:50051' }",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("blockchain::protocol".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Boolean(true));
	}
}
