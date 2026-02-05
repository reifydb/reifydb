// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::ringbuffer::RingBufferToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackRingBufferChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::plan::physical::CreateRingBufferNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_ringbuffer(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateRingBufferNode,
) -> crate::Result<Columns> {
	// Check if ring buffer already exists using the catalog
	if let Some(_) =
		services.catalog.find_ringbuffer_by_name(txn, plan.namespace.def().id, plan.ringbuffer.text())?
	{
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
			fragment: Some(plan.ringbuffer.clone()),
			ringbuffer: plan.ringbuffer.text().to_string(),
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
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::{
		catalog::{id::NamespaceId, namespace::NamespaceDef},
		resolved::ResolvedNamespace,
	};
	use reifydb_rql::plan::physical::PhysicalPlan;
	use reifydb_type::{fragment::Fragment, params::Params, value::Value};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{executor::Executor, instruction::ddl::create::ringbuffer::CreateRingBufferNode},
	};

	#[test]
	fn test_create_ringbuffer() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let resolved_namespace =
			ResolvedNamespace::new(Fragment::internal("test_namespace"), namespace.clone());

		let mut plan = CreateRingBufferNode {
			namespace: resolved_namespace.clone(),
			ringbuffer: Fragment::internal("test_ringbuffer"),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
			primary_key: None,
		};

		// First creation should succeed
		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Creating the same ring buffer again with `if_not_exists =
		// true` should not error
		plan.if_not_exists = true;
		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(false));

		// Creating the same ring buffer again with `if_not_exists =
		// false` should return error
		plan.if_not_exists = false;
		let err = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan), Params::default())
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_005");
	}

	#[test]
	fn test_create_same_ringbuffer_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

		let namespace_ident = Fragment::internal("test_namespace");
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
		let plan = CreateRingBufferNode {
			namespace: resolved_namespace,
			ringbuffer: Fragment::internal("test_ringbuffer"),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
			primary_key: None,
		};

		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
		let namespace_ident = Fragment::internal("another_schema");
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, another_schema.clone());
		let plan = CreateRingBufferNode {
			namespace: resolved_namespace,
			ringbuffer: Fragment::internal("test_ringbuffer"),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
			primary_key: None,
		};

		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_ringbuffer".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}

	#[test]
	fn test_create_ringbuffer_missing_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let namespace_ident = Fragment::internal("missing_schema");
		let namespace_def = NamespaceDef {
			id: NamespaceId(999),
			name: "missing_schema".to_string(),
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace_def);
		let plan = CreateRingBufferNode {
			namespace: resolved_namespace,
			ringbuffer: Fragment::internal("my_ringbuffer"),
			if_not_exists: false,
			columns: vec![],
			capacity: 1000,
			primary_key: None,
		};

		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateRingBuffer(plan), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("missing_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("my_ringbuffer".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}
}
