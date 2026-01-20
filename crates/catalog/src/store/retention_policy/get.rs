// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, primitive::PrimitiveId},
	retention::RetentionPolicy,
	internal,
};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::error::Error;

use crate::CatalogStore;

impl CatalogStore {
	/// Get a retention policy for a source (table, view, or ring buffer)
	/// Returns an error if no retention policy is set
	pub(crate) fn get_primitive_retention_policy(
		txn: &mut impl IntoStandardTransaction,
		source: PrimitiveId,
	) -> crate::Result<RetentionPolicy> {
		Self::find_primitive_retention_policy(txn, source)?.ok_or_else(|| {
			Error(internal!(
				"Retention policy for source {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				source
			))
		})
	}

	/// Get a retention policy for an operator (flow node)
	/// Returns an error if no retention policy is set
	pub(crate) fn get_operator_retention_policy(
		txn: &mut impl IntoStandardTransaction,
		operator: FlowNodeId,
	) -> crate::Result<RetentionPolicy> {
		Self::find_operator_retention_policy(txn, operator)?.ok_or_else(|| {
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
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::*;
	use crate::store::retention_policy::create::{
		_create_operator_retention_policy, create_primitive_retention_policy,
	};

	#[test]
	fn test_get_primitive_retention_policy_exists() {
		let mut txn = create_test_command_transaction();
		let source = PrimitiveId::View(ViewId(100));

		let policy = RetentionPolicy::KeepForever;

		create_primitive_retention_policy(&mut txn, source, &policy).unwrap();

		let retrieved = CatalogStore::get_primitive_retention_policy(&mut txn, source).unwrap();
		assert_eq!(retrieved, policy);
	}

	#[test]
	fn test_get_primitive_retention_policy_not_exists() {
		let mut txn = create_test_command_transaction();
		let source = PrimitiveId::RingBuffer(RingBufferId(9999));

		let err = CatalogStore::get_primitive_retention_policy(&mut txn, source).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("Retention policy"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[test]
	fn test_get_operator_retention_policy_exists() {
		let mut txn = create_test_command_transaction();
		let operator = FlowNodeId(777);

		let policy = RetentionPolicy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Delete,
		};

		_create_operator_retention_policy(&mut txn, operator, &policy).unwrap();

		let retrieved = CatalogStore::get_operator_retention_policy(&mut txn, operator).unwrap();
		assert_eq!(retrieved, policy);
	}

	#[test]
	fn test_get_operator_retention_policy_not_exists() {
		let mut txn = create_test_command_transaction();
		let operator = FlowNodeId(9999);

		let err = CatalogStore::get_operator_retention_policy(&mut txn, operator).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("Retention policy"));
		assert!(err.message.contains("not found in catalog"));
	}
}
