// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{catalog::ringbuffer::RingBufferToCreate, store::ttl::create::create_row_ttl};
use reifydb_core::{
	interface::catalog::{change::CatalogTrackRingBufferChangeOperations, shape::ShapeId},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateRingBufferNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_ringbuffer(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateRingBufferNode,
) -> Result<Columns> {
	// Check if ring buffer already exists using the catalog
	if let Some(existing) = services.catalog.find_ringbuffer_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.def().id(),
		plan.ringbuffer.text(),
	)? && plan.if_not_exists
	{
		return Ok(Columns::single_row([
			("id", Value::Uint8(existing.id.0)),
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("ringbuffer", Value::Utf8(plan.ringbuffer.text().to_string())),
			("created", Value::Boolean(false)),
		]));
		// The error will be returned by create_ringbuffer if
		// the ring buffer exists
	}

	let result = services.catalog.create_ringbuffer(
		txn,
		RingBufferToCreate {
			name: plan.ringbuffer.clone(),
			namespace: plan.namespace.def().id(),
			columns: plan.columns,
			capacity: plan.capacity,
			partition_by: plan.partition_by,
		},
	)?;
	let id = result.id;

	if let Some(ttl) = plan.ttl {
		create_row_ttl(txn, ShapeId::RingBuffer(id), &ttl)?;
	}

	txn.track_ringbuffer_created(result)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("ringbuffer", Value::Utf8(plan.ringbuffer.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::{params::Params, value::Value};

	use crate::{
		test_harness::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_ringbuffer() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		// Create namespace first
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		// First creation should succeed
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE RINGBUFFER test_namespace::test_ringbuffer { id: Int4 } WITH { capacity: 1000 }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(1025));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));

		// Creating the same ring buffer again should return error
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE RINGBUFFER test_namespace::test_ringbuffer { id: Int4 } WITH { capacity: 1000 }",
				params: Params::default(),
			},
		);
		assert!(r.is_err());
		assert_eq!(r.error.unwrap().diagnostic().code, "CA_005");
	}

	#[test]
	fn test_create_same_ringbuffer_in_different_shape() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		// Create both namespaces
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE another_shape",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		// Create ringbuffer in first namespace
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE RINGBUFFER test_namespace::test_ringbuffer { id: Int4 } WITH { capacity: 1000 }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(1025));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));

		// Create ringbuffer with same name in different namespace
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE RINGBUFFER another_shape::test_ringbuffer { id: Int4 } WITH { capacity: 1000 }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(1026));
		assert_eq!(frame[1].get_value(0), Value::Utf8("another_shape".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));
	}
}
