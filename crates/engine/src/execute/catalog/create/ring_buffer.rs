// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	ring_buffer::create::RingBufferToCreate,
	transaction::{CatalogRingBufferCommandOperations, CatalogRingBufferQueryOperations},
};
use reifydb_core::value::column::Columns;
use reifydb_rql::plan::physical::CreateRingBufferNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_ring_buffer<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateRingBufferNode,
	) -> crate::Result<Columns<'a>> {
		// Check if ring buffer already exists using the transaction's
		// catalog operations
		if let Some(_) = txn.find_ring_buffer_by_name(plan.namespace.def().id, plan.ring_buffer.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name().to_string())),
					("ring_buffer", Value::Utf8(plan.ring_buffer.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
			// The error will be returned by create_ring_buffer if
			// the ring buffer exists
		}

		txn.create_ring_buffer(RingBufferToCreate {
			fragment: Some(plan.ring_buffer.clone().into_owned()),
			ring_buffer: plan.ring_buffer.text().to_string(),
			namespace: plan.namespace.def().id,
			columns: plan.columns,
			capacity: plan.capacity,
		})?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("ring_buffer", Value::Utf8(plan.ring_buffer.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::{NamespaceDef, NamespaceId, Params, resolved::ResolvedNamespace};
	use reifydb_rql::plan::physical::PhysicalPlan;
	use reifydb_type::{Fragment, Value};

	use crate::{
		execute::{Executor, catalog::create::ring_buffer::CreateRingBufferNode},
		test_utils::create_test_command_transaction,
	};

	#[test]
	fn test_create_ring_buffer() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let resolved_namespace =
			ResolvedNamespace::new(Fragment::owned_internal("test_namespace"), namespace.clone());

		let mut plan = CreateRingBufferNode {
			namespace: resolved_namespace.clone(),
			ring_buffer: Fragment::owned_internal("test_ring_buffer"),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
		};

		// First creation should succeed
		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

		// Creating the same ring buffer again with `if_not_exists =
		// true` should not error
		plan.if_not_exists = true;
		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(false));

		// Creating the same ring buffer again with `if_not_exists =
		// false` should return error
		plan.if_not_exists = false;
		let err = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan), Params::default())
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_005");
	}

	#[test]
	fn test_create_same_ring_buffer_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

		let namespace_ident = Fragment::owned_internal("test_namespace");
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
		let plan = CreateRingBufferNode {
			namespace: resolved_namespace,
			ring_buffer: Fragment::owned_internal("test_ring_buffer"),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
		};

		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
		let namespace_ident = Fragment::owned_internal("another_schema");
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, another_schema.clone());
		let plan = CreateRingBufferNode {
			namespace: resolved_namespace,
			ring_buffer: Fragment::owned_internal("test_ring_buffer"),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
		};

		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}

	#[test]
	fn test_create_ring_buffer_missing_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let namespace_ident = Fragment::owned_internal("missing_schema");
		let namespace_def = NamespaceDef {
			id: NamespaceId(999),
			name: "missing_schema".to_string(),
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace_def);
		let plan = CreateRingBufferNode {
			namespace: resolved_namespace,
			ring_buffer: Fragment::owned_internal("my_ring_buffer"),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
		};

		// With defensive fallback, this now succeeds even with
		// non-existent namespace The ring buffer is created with the
		// provided namespace ID
		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan), Params::default())
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("missing_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("my_ring_buffer".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}
}
