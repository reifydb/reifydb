// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId, retention::RetentionStrategy};

use crate::materialized::{MaterializedCatalog, MultiVersionRetentionStrategy};

impl MaterializedCatalog {
	/// Find a retention strategy for an operator at a specific version
	pub fn find_operator_retention_strategy_at(
		&self,
		operator: FlowNodeId,
		version: CommitVersion,
	) -> Option<RetentionStrategy> {
		self.operator_retention_strategies.get(&operator).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a retention strategy for an operator (returns latest version)
	pub fn find_operator_retention_strategy(&self, operator: FlowNodeId) -> Option<RetentionStrategy> {
		self.operator_retention_strategies.get(&operator).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Set a retention strategy for an operator at a specific version
	pub fn set_operator_retention_strategy(
		&self,
		operator: FlowNodeId,
		version: CommitVersion,
		policy: Option<RetentionStrategy>,
	) {
		let multi = self
			.operator_retention_strategies
			.get_or_insert_with(operator, MultiVersionRetentionStrategy::new);

		if let Some(new_policy) = policy {
			multi.value().insert(version, new_policy);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::retention::{CleanupMode, RetentionStrategy};

	use super::*;

	#[test]
	fn test_set_and_find_operator_retention_strategy() {
		let catalog = MaterializedCatalog::new();
		let operator = FlowNodeId(100);
		let policy = RetentionStrategy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Drop,
		};

		// Set policy at version 1
		catalog.set_operator_retention_strategy(operator, CommitVersion(1), Some(policy.clone()));

		// Find policy at version 1
		let found = catalog.find_operator_retention_strategy_at(operator, CommitVersion(1));
		assert_eq!(found, Some(policy.clone()));

		// Find policy at later version (should return same policy)
		let found = catalog.find_operator_retention_strategy_at(operator, CommitVersion(5));
		assert_eq!(found, Some(policy));

		// Policy shouldn't exist at version 0
		let found = catalog.find_operator_retention_strategy_at(operator, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_operator_retention_strategy_update() {
		let catalog = MaterializedCatalog::new();
		let operator = FlowNodeId(42);

		// Set initial policy
		let policy_v1 = RetentionStrategy::KeepForever;
		catalog.set_operator_retention_strategy(operator, CommitVersion(1), Some(policy_v1.clone()));

		// Verify initial state
		assert_eq!(
			catalog.find_operator_retention_strategy_at(operator, CommitVersion(1)),
			Some(policy_v1.clone())
		);

		// Update policy
		let policy_v2 = RetentionStrategy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Delete,
		};
		catalog.set_operator_retention_strategy(operator, CommitVersion(2), Some(policy_v2.clone()));

		// Historical query at version 1 should still show old policy
		assert_eq!(catalog.find_operator_retention_strategy_at(operator, CommitVersion(1)), Some(policy_v1));

		// Current version should show new policy
		assert_eq!(
			catalog.find_operator_retention_strategy_at(operator, CommitVersion(2)),
			Some(policy_v2.clone())
		);
		assert_eq!(catalog.find_operator_retention_strategy_at(operator, CommitVersion(10)), Some(policy_v2));
	}

	#[test]
	fn test_operator_retention_strategy_deletion() {
		let catalog = MaterializedCatalog::new();
		let operator = FlowNodeId(999);

		// Create and set policy
		let policy = RetentionStrategy::KeepVersions {
			count: 100,
			cleanup_mode: CleanupMode::Drop,
		};
		catalog.set_operator_retention_strategy(operator, CommitVersion(1), Some(policy.clone()));

		// Verify it exists
		assert_eq!(
			catalog.find_operator_retention_strategy_at(operator, CommitVersion(1)),
			Some(policy.clone())
		);

		// Delete the policy
		catalog.set_operator_retention_strategy(operator, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_operator_retention_strategy_at(operator, CommitVersion(2)), None);

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_operator_retention_strategy_at(operator, CommitVersion(1)), Some(policy));
	}

	#[test]
	fn test_operator_retention_strategy_versioning() {
		let catalog = MaterializedCatalog::new();
		let operator = FlowNodeId(777);

		// Create multiple versions
		let policy_v1 = RetentionStrategy::KeepForever;
		let policy_v2 = RetentionStrategy::KeepVersions {
			count: 2,
			cleanup_mode: CleanupMode::Delete,
		};
		let policy_v3 = RetentionStrategy::KeepVersions {
			count: 50,
			cleanup_mode: CleanupMode::Drop,
		};

		// Set at different versions
		catalog.set_operator_retention_strategy(operator, CommitVersion(10), Some(policy_v1.clone()));
		catalog.set_operator_retention_strategy(operator, CommitVersion(20), Some(policy_v2.clone()));
		catalog.set_operator_retention_strategy(operator, CommitVersion(30), Some(policy_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_operator_retention_strategy_at(operator, CommitVersion(5)), None);
		assert_eq!(
			catalog.find_operator_retention_strategy_at(operator, CommitVersion(10)),
			Some(policy_v1.clone())
		);
		assert_eq!(catalog.find_operator_retention_strategy_at(operator, CommitVersion(15)), Some(policy_v1));
		assert_eq!(
			catalog.find_operator_retention_strategy_at(operator, CommitVersion(20)),
			Some(policy_v2.clone())
		);
		assert_eq!(catalog.find_operator_retention_strategy_at(operator, CommitVersion(25)), Some(policy_v2));
		assert_eq!(
			catalog.find_operator_retention_strategy_at(operator, CommitVersion(30)),
			Some(policy_v3.clone())
		);
		assert_eq!(catalog.find_operator_retention_strategy_at(operator, CommitVersion(100)), Some(policy_v3));
	}
}
