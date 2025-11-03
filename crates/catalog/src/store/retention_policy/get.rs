// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{FlowNodeId, QueryTransaction, SourceId},
	retention::RetentionPolicy,
};
use reifydb_type::internal_error;

use crate::CatalogStore;

impl CatalogStore {
	/// Get a retention policy for a source (table, view, or ring buffer)
	/// Returns an error if no retention policy is set
	pub fn get_source_retention_policy(
		txn: &mut impl QueryTransaction,
		source: SourceId,
	) -> crate::Result<RetentionPolicy> {
		Self::find_source_retention_policy(txn, source)?.ok_or_else(|| {
			Error(internal_error!(
				"Retention policy for source {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				source
			))
		})
	}

	/// Get a retention policy for an operator (flow node)
	/// Returns an error if no retention policy is set
	pub fn get_operator_retention_policy(
		txn: &mut impl QueryTransaction,
		operator: FlowNodeId,
	) -> crate::Result<RetentionPolicy> {
		Self::find_operator_retention_policy(txn, operator)?.ok_or_else(|| {
			Error(internal_error!(
				"Retention policy for operator {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				operator
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{RingBufferId, ViewId},
		retention::{CleanupMode, RetentionPolicy},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::*;
	use crate::store::retention_policy::create::{
		_create_operator_retention_policy, create_source_retention_policy,
	};

	#[test]
	fn test_get_source_retention_policy_exists() {
		let mut txn = create_test_command_transaction();
		let source = SourceId::View(ViewId(100));

		let policy = RetentionPolicy::KeepForever;

		create_source_retention_policy(&mut txn, source, &policy).unwrap();

		let retrieved = CatalogStore::get_source_retention_policy(&mut txn, source).unwrap();
		assert_eq!(retrieved, policy);
	}

	#[test]
	fn test_get_source_retention_policy_not_exists() {
		let mut txn = create_test_command_transaction();
		let source = SourceId::RingBuffer(RingBufferId(9999));

		let err = CatalogStore::get_source_retention_policy(&mut txn, source).unwrap_err();

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
