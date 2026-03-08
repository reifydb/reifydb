// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::AlterRemoteNamespaceNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn alter_remote_namespace(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterRemoteNamespaceNode,
) -> Result<Columns> {
	let ns_name = plan.namespace.text();
	let ns_def = services.catalog.get_namespace_by_name(&mut Transaction::Admin(txn), ns_name)?;
	let grpc_text = plan.grpc.text().to_string();

	services.catalog.update_namespace_grpc(txn, ns_def.id(), Some(grpc_text))?;

	Ok(Columns::single_row([("namespace", Value::Utf8(ns_name.to_string())), ("altered", Value::Boolean(true))]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::{
		params::Params,
		value::{Value, identity::IdentityId},
	};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_alter_remote_namespace() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = IdentityId::root();

		// First create a remote namespace
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE REMOTE NAMESPACE remote_ns WITH { grpc: 'localhost:50051' }",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();

		// Alter the grpc address
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "ALTER REMOTE NAMESPACE remote_ns WITH { grpc: 'newhost:50051' }",
					params: Params::default(),
					identity,
				},
			)
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("remote_ns".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Boolean(true));
	}
}
