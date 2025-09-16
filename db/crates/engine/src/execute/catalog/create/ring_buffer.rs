// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	ring_buffer::create::RingBufferToCreate,
	transaction::{CatalogRingBufferCommandOperations, CatalogRingBufferQueryOperations},
};
use reifydb_core::{interface::Transaction, value::columnar::Columns};
use reifydb_rql::plan::physical::CreateRingBufferPlan;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_ring_buffer<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateRingBufferPlan,
	) -> crate::Result<Columns> {
		// Check if ring buffer already exists using the transaction's
		// catalog operations
		if let Some(_) = txn.find_ring_buffer_by_name(plan.namespace.id, plan.ring_buffer.name.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("ring_buffer", Value::Utf8(plan.ring_buffer.name.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
			// The error will be returned by create_ring_buffer if
			// the ring buffer exists
		}

		txn.create_ring_buffer(RingBufferToCreate {
			fragment: Some(plan.ring_buffer.name.clone().into_owned()),
			ring_buffer: plan.ring_buffer.name.text().to_string(),
			namespace: plan.namespace.id,
			columns: plan.columns,
			capacity: plan.capacity,
		})?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("ring_buffer", Value::Utf8(plan.ring_buffer.name.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::{NamespaceDef, NamespaceId, Params, RingBufferIdentifier};
	use reifydb_rql::plan::physical::PhysicalPlan;
	use reifydb_type::{Fragment, Value};

	use crate::{
		execute::{Executor, catalog::create::ring_buffer::CreateRingBufferPlan},
		test_utils::create_test_command_transaction,
	};

	#[test]
	fn test_create_ring_buffer() {
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let mut plan = CreateRingBufferPlan {
			namespace: NamespaceDef {
				id: namespace.id,
				name: namespace.name.clone(),
			},
			ring_buffer: RingBufferIdentifier::new(
				Fragment::owned_internal("test_namespace"),
				Fragment::owned_internal("test_ring_buffer"),
			),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
		};

		// First creation should succeed
		let result = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

		// Creating the same ring buffer again with `if_not_exists =
		// true` should not error
		plan.if_not_exists = true;
		let result = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(false));

		// Creating the same ring buffer again with `if_not_exists =
		// false` should return error
		plan.if_not_exists = false;
		let err = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan), Params::default())
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_005");
	}

	#[test]
	fn test_create_same_ring_buffer_in_different_schema() {
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

		let plan = CreateRingBufferPlan {
			namespace: NamespaceDef {
				id: namespace.id,
				name: namespace.name.clone(),
			},
			ring_buffer: RingBufferIdentifier::new(
				Fragment::owned_internal("test_namespace"),
				Fragment::owned_internal("test_ring_buffer"),
			),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
		};

		let result = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
		let plan = CreateRingBufferPlan {
			namespace: NamespaceDef {
				id: another_schema.id,
				name: another_schema.name.clone(),
			},
			ring_buffer: RingBufferIdentifier::new(
				Fragment::owned_internal("another_schema"),
				Fragment::owned_internal("test_ring_buffer"),
			),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
		};

		let result = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}

	#[test]
	fn test_create_ring_buffer_missing_schema() {
		let mut txn = create_test_command_transaction();

		let plan = CreateRingBufferPlan {
			namespace: NamespaceDef {
				id: NamespaceId(999),
				name: "missing_schema".to_string(),
			},
			ring_buffer: RingBufferIdentifier::new(
				Fragment::owned_internal("missing_schema"),
				Fragment::owned_internal("my_ring_buffer"),
			),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
		};

		// With defensive fallback, this now succeeds even with
		// non-existent namespace The ring buffer is created with the
		// provided namespace ID
		let result = Executor::testing()
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("missing_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("my_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}
}
