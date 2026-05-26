// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId, row::OperatorSettings};

use crate::cache::{CatalogCache, MultiVersionOperatorSettings};

impl CatalogCache {
	pub fn find_operator_settings_at(
		&self,
		operator: FlowNodeId,
		version: CommitVersion,
	) -> Option<OperatorSettings> {
		self.operator_settings.get(&operator).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_operator_settings(&self, operator: FlowNodeId) -> Option<OperatorSettings> {
		self.operator_settings.get(&operator).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_operator_settings(
		&self,
		operator: FlowNodeId,
		version: CommitVersion,
		settings: Option<OperatorSettings>,
	) {
		let multi = self.operator_settings.get_or_insert_with(operator, MultiVersionOperatorSettings::new);

		if let Some(new_settings) = settings {
			multi.value().insert(version, new_settings);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::row::{Ttl, TtlAnchor, TtlCleanupMode};

	use super::*;

	fn settings(duration_nanos: u64, anchor: TtlAnchor, cleanup_mode: TtlCleanupMode) -> OperatorSettings {
		OperatorSettings {
			ttl: Some(Ttl {
				duration_nanos,
				anchor,
				cleanup_mode,
			}),
			join: None,
		}
	}

	#[test]
	fn test_set_and_find_operator_settings() {
		let catalog = CatalogCache::new();
		let operator = FlowNodeId(1);
		let config = settings(300_000_000_000, TtlAnchor::Created, TtlCleanupMode::Drop);

		catalog.set_operator_settings(operator, CommitVersion(1), Some(config.clone()));

		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(1)), Some(config.clone()));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(5)), Some(config));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(0)), None);
	}

	#[test]
	fn test_operator_settings_versioning_and_deletion() {
		let catalog = CatalogCache::new();
		let operator = FlowNodeId(42);

		let v1 = settings(300_000_000_000, TtlAnchor::Created, TtlCleanupMode::Drop);
		let v2 = settings(600_000_000_000, TtlAnchor::Updated, TtlCleanupMode::Delete);

		catalog.set_operator_settings(operator, CommitVersion(1), Some(v1.clone()));
		catalog.set_operator_settings(operator, CommitVersion(2), Some(v2.clone()));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(1)), Some(v1));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(10)), Some(v2.clone()));

		catalog.set_operator_settings(operator, CommitVersion(3), None);
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(3)), None);
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(2)), Some(v2));
	}
}
