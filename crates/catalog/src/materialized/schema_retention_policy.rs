// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::schema::SchemaId, retention::RetentionPolicy};

use crate::materialized::{MaterializedCatalog, MultiVersionRetentionPolicy};

impl MaterializedCatalog {
	/// Find a retention policy for a schema at a specific version
	pub fn find_schema_retention_policy_at(
		&self,
		schema: SchemaId,
		version: CommitVersion,
	) -> Option<RetentionPolicy> {
		self.schema_retention_policies.get(&schema).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a retention policy for a schema (returns latest version)
	pub fn find_schema_retention_policy(&self, schema: SchemaId) -> Option<RetentionPolicy> {
		self.schema_retention_policies.get(&schema).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Set a retention policy for a schema at a specific version
	pub fn set_schema_retention_policy(
		&self,
		schema: SchemaId,
		version: CommitVersion,
		policy: Option<RetentionPolicy>,
	) {
		let multi = self.schema_retention_policies.get_or_insert_with(schema, MultiVersionRetentionPolicy::new);

		if let Some(new_policy) = policy {
			multi.value().insert(version, new_policy);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		config::SystemConfig,
		interface::catalog::id::TableId,
		retention::{CleanupMode, RetentionPolicy},
	};

	use super::*;

	#[test]
	fn test_set_and_find_schema_retention_policy() {
		let catalog = MaterializedCatalog::new(SystemConfig::new());
		let schema = SchemaId::Table(TableId(1));
		let policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		// Set policy at version 1
		catalog.set_schema_retention_policy(schema, CommitVersion(1), Some(policy.clone()));

		// Find policy at version 1
		let found = catalog.find_schema_retention_policy_at(schema, CommitVersion(1));
		assert_eq!(found, Some(policy.clone()));

		// Find policy at later version (should return same policy)
		let found = catalog.find_schema_retention_policy_at(schema, CommitVersion(5));
		assert_eq!(found, Some(policy));

		// Policy shouldn't exist at version 0
		let found = catalog.find_schema_retention_policy_at(schema, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_schema_retention_policy_update() {
		let catalog = MaterializedCatalog::new(SystemConfig::new());
		let schema = SchemaId::Table(TableId(42));

		// Set initial policy
		let policy_v1 = RetentionPolicy::KeepForever;
		catalog.set_schema_retention_policy(schema, CommitVersion(1), Some(policy_v1.clone()));

		// Verify initial state
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(1)), Some(policy_v1.clone()));

		// Update policy
		let policy_v2 = RetentionPolicy::KeepVersions {
			count: 20,
			cleanup_mode: CleanupMode::Drop,
		};
		catalog.set_schema_retention_policy(schema, CommitVersion(2), Some(policy_v2.clone()));

		// Historical query at version 1 should still show old policy
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(1)), Some(policy_v1));

		// Current version should show new policy
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(2)), Some(policy_v2.clone()));
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(10)), Some(policy_v2));
	}

	#[test]
	fn test_schema_retention_policy_deletion() {
		let catalog = MaterializedCatalog::new(SystemConfig::new());
		let schema = SchemaId::Table(TableId(99));

		// Create and set policy
		let policy = RetentionPolicy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Delete,
		};
		catalog.set_schema_retention_policy(schema, CommitVersion(1), Some(policy.clone()));

		// Verify it exists
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(1)), Some(policy.clone()));

		// Delete the policy
		catalog.set_schema_retention_policy(schema, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(2)), None);

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(1)), Some(policy));
	}

	#[test]
	fn test_schema_retention_policy_versioning() {
		let catalog = MaterializedCatalog::new(SystemConfig::new());
		let schema = SchemaId::Table(TableId(100));

		// Create multiple versions
		let policy_v1 = RetentionPolicy::KeepForever;
		let policy_v2 = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};
		let policy_v3 = RetentionPolicy::KeepVersions {
			count: 100,
			cleanup_mode: CleanupMode::Drop,
		};

		// Set at different versions
		catalog.set_schema_retention_policy(schema, CommitVersion(10), Some(policy_v1.clone()));
		catalog.set_schema_retention_policy(schema, CommitVersion(20), Some(policy_v2.clone()));
		catalog.set_schema_retention_policy(schema, CommitVersion(30), Some(policy_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(5)), None);
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(10)), Some(policy_v1.clone()));
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(15)), Some(policy_v1));
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(20)), Some(policy_v2.clone()));
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(25)), Some(policy_v2));
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(30)), Some(policy_v3.clone()));
		assert_eq!(catalog.find_schema_retention_policy_at(schema, CommitVersion(100)), Some(policy_v3));
	}
}
