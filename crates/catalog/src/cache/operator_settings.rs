// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::OperatorId, row::OperatorSettings};

use crate::cache::{CatalogCache, MultiVersionOperatorSettings};

impl CatalogCache {
	pub fn find_operator_settings_at(
		&self,
		operator: OperatorId,
		version: CommitVersion,
	) -> Option<OperatorSettings> {
		self.operator_settings.get(&operator).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_operator_settings(&self, operator: OperatorId) -> Option<OperatorSettings> {
		self.operator_settings.get(&operator).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_operator_settings(
		&self,
		operator: OperatorId,
		version: CommitVersion,
		settings: Option<OperatorSettings>,
	) {
		let _guard = self.write_lock.lock();
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
	use reifydb_core::row::OperatorLateness;
	use reifydb_value::value::duration::Duration;

	use super::*;

	fn settings(duration: Duration) -> OperatorSettings {
		OperatorSettings {
			lateness: Some(OperatorLateness {
				duration,
			}),
			join: None,
		}
	}

	#[test]
	fn test_set_and_find_operator_settings() {
		let catalog = CatalogCache::new();
		let operator = OperatorId(1);
		let config = settings(Duration::from_minutes(5).unwrap());

		catalog.set_operator_settings(operator, CommitVersion(1), Some(config.clone()));

		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(1)), Some(config.clone()));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(5)), Some(config));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(0)), None);
	}

	#[test]
	fn test_operator_settings_versioning_and_deletion() {
		let catalog = CatalogCache::new();
		let operator = OperatorId(42);

		let v1 = settings(Duration::from_minutes(5).unwrap());
		let v2 = settings(Duration::from_minutes(10).unwrap());

		catalog.set_operator_settings(operator, CommitVersion(1), Some(v1.clone()));
		catalog.set_operator_settings(operator, CommitVersion(2), Some(v2.clone()));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(1)), Some(v1));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(10)), Some(v2.clone()));

		catalog.set_operator_settings(operator, CommitVersion(3), None);
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(3)), None);
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(2)), Some(v2));
	}

	#[test]
	fn latest_read_finds_settings_written_after_reader_version() {
		// Settings can commit at a later version than the registering transaction's snapshot.
		// A version-pinned read then returns none and the operator's tick-eviction never runs,
		// leaking per-row maps, so registration must use the latest read.
		let catalog = CatalogCache::new();
		let operator = OperatorId(7);
		let cfg = settings(Duration::from_seconds(10).unwrap());

		catalog.set_operator_settings(operator, CommitVersion(5), Some(cfg.clone()));

		assert_eq!(
			catalog.find_operator_settings_at(operator, CommitVersion(3)),
			None,
			"a reader pinned to an earlier version misses settings committed later - this is the bug"
		);
		assert_eq!(
			catalog.find_operator_settings(operator),
			Some(cfg),
			"the latest read must find settings regardless of reader version - this is the fix"
		);
	}
}
