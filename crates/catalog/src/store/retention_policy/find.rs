// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{FlowNodeId, QueryTransaction, SourceId},
	key::{OperatorRetentionPolicyKey, SourceRetentionPolicyKey},
	retention::RetentionPolicy,
};

use super::decode_retention_policy;
use crate::CatalogStore;

impl CatalogStore {
	/// Find a retention policy for a source (table, view, or ring buffer)
	/// Returns None if no retention policy is set
	pub async fn find_source_retention_policy(
		txn: &mut impl QueryTransaction,
		source: SourceId,
	) -> crate::Result<Option<RetentionPolicy>> {
		let value = txn.get(&SourceRetentionPolicyKey::encoded(source)).await?;
		Ok(value.and_then(|v| decode_retention_policy(&v.values)))
	}

	/// Find a retention policy for an operator
	/// Returns None if no retention policy is set
	pub async fn find_operator_retention_policy(
		txn: &mut impl QueryTransaction,
		operator: FlowNodeId,
	) -> crate::Result<Option<RetentionPolicy>> {
		let value = txn.get(&OperatorRetentionPolicyKey::encoded(operator)).await?;
		Ok(value.and_then(|v| decode_retention_policy(&v.values)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::TableId,
		retention::{CleanupMode, RetentionPolicy},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::*;
	use crate::store::retention_policy::create::{
		_create_operator_retention_policy, create_source_retention_policy,
	};

	#[tokio::test]
	fn test_find_source_retention_policy_exists() {
		let mut txn = create_test_command_transaction().await;
		let source = SourceId::Table(TableId(42));

		let policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_source_retention_policy(&mut txn, source, &policy).unwrap();

		let found = CatalogStore::find_source_retention_policy(&mut txn, source).unwrap();
		assert_eq!(found, Some(policy));
	}

	#[tokio::test]
	fn test_find_source_retention_policy_not_exists() {
		let mut txn = create_test_command_transaction().await;
		let source = SourceId::Table(TableId(9999));

		let found = CatalogStore::find_source_retention_policy(&mut txn, source).unwrap();
		assert_eq!(found, None);
	}

	#[tokio::test]
	fn test_find_operator_retention_policy_exists() {
		let mut txn = create_test_command_transaction().await;
		let operator = FlowNodeId(999);

		let policy = RetentionPolicy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Drop,
		};

		_create_operator_retention_policy(&mut txn, operator, &policy).unwrap();

		let found = CatalogStore::find_operator_retention_policy(&mut txn, operator).unwrap();
		assert_eq!(found, Some(policy));
	}

	#[tokio::test]
	fn test_find_operator_retention_policy_not_exists() {
		let mut txn = create_test_command_transaction().await;
		let operator = FlowNodeId(9999);

		let found = CatalogStore::find_operator_retention_policy(&mut txn, operator).unwrap();
		assert_eq!(found, None);
	}
}
