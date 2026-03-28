// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, schema::SchemaId},
	internal,
	retention::RetentionPolicy,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	/// Get a retention policy for a schema (table, view, or ring buffer)
	/// Returns an error if no retention policy is set
	pub(crate) fn get_schema_retention_policy(
		rx: &mut Transaction<'_>,
		schema: SchemaId,
	) -> Result<RetentionPolicy> {
		Self::find_schema_retention_policy(rx, schema)?.ok_or_else(|| {
			Error(internal!(
				"Retention policy for schema {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				schema
			))
		})
	}

	/// Get a retention policy for an operator (flow node)
	/// Returns an error if no retention policy is set
	pub(crate) fn get_operator_retention_policy(
		rx: &mut Transaction<'_>,
		operator: FlowNodeId,
	) -> Result<RetentionPolicy> {
		Self::find_operator_retention_policy(rx, operator)?.ok_or_else(|| {
			Error(internal!(
				"Retention policy for operator {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				operator
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{RingBufferId, ViewId},
		retention::{CleanupMode, RetentionPolicy},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::store::retention_policy::create::{
		_create_operator_retention_policy, create_schema_retention_policy,
	};

	#[test]
	fn test_get_schema_retention_policy_exists() {
		let mut txn = create_test_admin_transaction();
		let schema = SchemaId::View(ViewId(100));

		let policy = RetentionPolicy::KeepForever;

		create_schema_retention_policy(&mut txn, schema, &policy).unwrap();

		let retrieved =
			CatalogStore::get_schema_retention_policy(&mut Transaction::Admin(&mut txn), schema).unwrap();
		assert_eq!(retrieved, policy);
	}

	#[test]
	fn test_get_schema_retention_policy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let schema = SchemaId::RingBuffer(RingBufferId(9999));

		let err = CatalogStore::get_schema_retention_policy(&mut Transaction::Admin(&mut txn), schema)
			.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("Retention policy"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[test]
	fn test_get_operator_retention_policy_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(777);

		let policy = RetentionPolicy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Delete,
		};

		_create_operator_retention_policy(&mut txn, operator, &policy).unwrap();

		let retrieved =
			CatalogStore::get_operator_retention_policy(&mut Transaction::Admin(&mut txn), operator)
				.unwrap();
		assert_eq!(retrieved, policy);
	}

	#[test]
	fn test_get_operator_retention_policy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(9999);

		let err = CatalogStore::get_operator_retention_policy(&mut Transaction::Admin(&mut txn), operator)
			.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("Retention policy"));
		assert!(err.message.contains("not found in catalog"));
	}
}
