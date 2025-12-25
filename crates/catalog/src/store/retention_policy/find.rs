// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{FlowNodeId, PrimitiveId, QueryTransaction},
	key::{OperatorRetentionPolicyKey, PrimitiveRetentionPolicyKey},
	retention::RetentionPolicy,
};

use super::decode_retention_policy;
use crate::CatalogStore;

impl CatalogStore {
	/// Find a retention policy for a source (table, view, or ring buffer)
	/// Returns None if no retention policy is set
	pub async fn find_primitive_retention_policy(
		txn: &mut impl QueryTransaction,
		source: PrimitiveId,
	) -> crate::Result<Option<RetentionPolicy>> {
		let value = txn.get(&PrimitiveRetentionPolicyKey::encoded(source)).await?;
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
		_create_operator_retention_policy, create_primitive_retention_policy,
	};

	#[tokio::test]
	async fn test_find_primitive_retention_policy_exists() {
		let mut txn = create_test_command_transaction().await;
		let source = PrimitiveId::Table(TableId(42));

		let policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_primitive_retention_policy(&mut txn, source, &policy).await.unwrap();

		let found = CatalogStore::find_primitive_retention_policy(&mut txn, source).await.unwrap();
		assert_eq!(found, Some(policy));
	}

	#[tokio::test]
	async fn test_find_primitive_retention_policy_not_exists() {
		let mut txn = create_test_command_transaction().await;
		let source = PrimitiveId::Table(TableId(9999));

		let found = CatalogStore::find_primitive_retention_policy(&mut txn, source).await.unwrap();
		assert_eq!(found, None);
	}

	#[tokio::test]
	async fn test_find_operator_retention_policy_exists() {
		let mut txn = create_test_command_transaction().await;
		let operator = FlowNodeId(999);

		let policy = RetentionPolicy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Drop,
		};

		_create_operator_retention_policy(&mut txn, operator, &policy).await.unwrap();

		let found = CatalogStore::find_operator_retention_policy(&mut txn, operator).await.unwrap();
		assert_eq!(found, Some(policy));
	}

	#[tokio::test]
	async fn test_find_operator_retention_policy_not_exists() {
		let mut txn = create_test_command_transaction().await;
		let operator = FlowNodeId(9999);

		let found = CatalogStore::find_operator_retention_policy(&mut txn, operator).await.unwrap();
		assert_eq!(found, None);
	}
}
