// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::ringbuffer::RingBufferToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackRingBufferChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateRingBufferNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_ringbuffer(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateRingBufferNode,
) -> crate::Result<Columns> {
	// Check if ring buffer already exists using the catalog
	if let Some(_) = services.catalog.find_ringbuffer_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.def().id,
		plan.ringbuffer.text(),
	)? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("ringbuffer", Value::Utf8(plan.ringbuffer.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
		// The error will be returned by create_ringbuffer if
		// the ring buffer exists
	}

	let result = services.catalog.create_ringbuffer(
		txn,
		RingBufferToCreate {
			name: plan.ringbuffer.clone(),
			namespace: plan.namespace.def().id,
			columns: plan.columns,
			capacity: plan.capacity,
		},
	)?;
	txn.track_ringbuffer_def_created(result)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("ringbuffer", Value::Utf8(plan.ringbuffer.text().to_string())),
		("created", Value::Boolean(true)),
	]))
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
	fn test_create_ringbuffer() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = IdentityId::root();

		// Create namespace first
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();

		// First creation should succeed
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE RINGBUFFER test_namespace::test_ringbuffer { id: Int4 } WITH { capacity: 1000 }",
					params: Params::default(),
					identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Creating the same ring buffer again should return error
		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE RINGBUFFER test_namespace::test_ringbuffer { id: Int4 } WITH { capacity: 1000 }",
					params: Params::default(),
					identity,
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_005");
	}

	#[test]
	fn test_create_same_ringbuffer_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = IdentityId::root();

		// Create both namespaces
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE another_schema",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();

		// Create ringbuffer in first namespace
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE RINGBUFFER test_namespace::test_ringbuffer { id: Int4 } WITH { capacity: 1000 }",
					params: Params::default(),
					identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Create ringbuffer with same name in different namespace
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE RINGBUFFER another_schema::test_ringbuffer { id: Int4 } WITH { capacity: 1000 }",
					params: Params::default(),
					identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}
}
