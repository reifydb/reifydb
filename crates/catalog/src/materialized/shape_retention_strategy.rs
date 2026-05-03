// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::shape::ShapeId, retention::RetentionStrategy};

use crate::materialized::{MaterializedCatalog, MultiVersionRetentionStrategy};

impl MaterializedCatalog {
	pub fn find_shape_retention_strategy_at(
		&self,
		shape: ShapeId,
		version: CommitVersion,
	) -> Option<RetentionStrategy> {
		self.shape_retention_strategies.get(&shape).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_shape_retention_strategy(&self, shape: ShapeId) -> Option<RetentionStrategy> {
		self.shape_retention_strategies.get(&shape).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_shape_retention_strategy(
		&self,
		shape: ShapeId,
		version: CommitVersion,
		strategy: Option<RetentionStrategy>,
	) {
		let multi =
			self.shape_retention_strategies.get_or_insert_with(shape, MultiVersionRetentionStrategy::new);

		if let Some(new_strategy) = strategy {
			multi.value().insert(version, new_strategy);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		retention::{CleanupMode, RetentionStrategy},
	};

	use super::*;

	#[test]
	fn test_set_and_find_shape_retention_strategy() {
		let catalog = MaterializedCatalog::new();
		let shape = ShapeId::Table(TableId(1));
		let policy = RetentionStrategy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		// Set policy at version 1
		catalog.set_shape_retention_strategy(shape, CommitVersion(1), Some(policy.clone()));

		// Find policy at version 1
		let found = catalog.find_shape_retention_strategy_at(shape, CommitVersion(1));
		assert_eq!(found, Some(policy.clone()));

		// Find policy at later version (should return same policy)
		let found = catalog.find_shape_retention_strategy_at(shape, CommitVersion(5));
		assert_eq!(found, Some(policy));

		// Policy shouldn't exist at version 0
		let found = catalog.find_shape_retention_strategy_at(shape, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_shape_retention_strategy_update() {
		let catalog = MaterializedCatalog::new();
		let shape = ShapeId::Table(TableId(42));

		// Set initial policy
		let policy_v1 = RetentionStrategy::KeepForever;
		catalog.set_shape_retention_strategy(shape, CommitVersion(1), Some(policy_v1.clone()));

		// Verify initial state
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(1)), Some(policy_v1.clone()));

		// Update policy
		let policy_v2 = RetentionStrategy::KeepVersions {
			count: 20,
			cleanup_mode: CleanupMode::Drop,
		};
		catalog.set_shape_retention_strategy(shape, CommitVersion(2), Some(policy_v2.clone()));

		// Historical query at version 1 should still show old policy
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(1)), Some(policy_v1));

		// Current version should show new policy
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(2)), Some(policy_v2.clone()));
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(10)), Some(policy_v2));
	}

	#[test]
	fn test_shape_retention_strategy_deletion() {
		let catalog = MaterializedCatalog::new();
		let shape = ShapeId::Table(TableId(99));

		// Create and set policy
		let policy = RetentionStrategy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Delete,
		};
		catalog.set_shape_retention_strategy(shape, CommitVersion(1), Some(policy.clone()));

		// Verify it exists
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(1)), Some(policy.clone()));

		// Delete the policy
		catalog.set_shape_retention_strategy(shape, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(2)), None);

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(1)), Some(policy));
	}

	#[test]
	fn test_shape_retention_strategy_versioning() {
		let catalog = MaterializedCatalog::new();
		let shape = ShapeId::Table(TableId(100));

		// Create multiple versions
		let policy_v1 = RetentionStrategy::KeepForever;
		let policy_v2 = RetentionStrategy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};
		let policy_v3 = RetentionStrategy::KeepVersions {
			count: 100,
			cleanup_mode: CleanupMode::Drop,
		};

		// Set at different versions
		catalog.set_shape_retention_strategy(shape, CommitVersion(10), Some(policy_v1.clone()));
		catalog.set_shape_retention_strategy(shape, CommitVersion(20), Some(policy_v2.clone()));
		catalog.set_shape_retention_strategy(shape, CommitVersion(30), Some(policy_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(5)), None);
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(10)), Some(policy_v1.clone()));
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(15)), Some(policy_v1));
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(20)), Some(policy_v2.clone()));
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(25)), Some(policy_v2));
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(30)), Some(policy_v3.clone()));
		assert_eq!(catalog.find_shape_retention_strategy_at(shape, CommitVersion(100)), Some(policy_v3));
	}
}
