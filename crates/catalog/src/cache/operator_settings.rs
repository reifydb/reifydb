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
	use reifydb_core::row::{Ttl, TtlCleanupMode};

	use super::*;

	fn settings(duration_nanos: u64, cleanup_mode: TtlCleanupMode) -> OperatorSettings {
		OperatorSettings {
			ttl: Some(Ttl {
				duration_nanos,
				cleanup_mode,
			}),
			join: None,
		}
	}

	#[test]
	fn test_set_and_find_operator_settings() {
		let catalog = CatalogCache::new();
		let operator = FlowNodeId(1);
		let config = settings(300_000_000_000, TtlCleanupMode::Drop);

		catalog.set_operator_settings(operator, CommitVersion(1), Some(config.clone()));

		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(1)), Some(config.clone()));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(5)), Some(config));
		assert_eq!(catalog.find_operator_settings_at(operator, CommitVersion(0)), None);
	}

	#[test]
	fn test_operator_settings_versioning_and_deletion() {
		let catalog = CatalogCache::new();
		let operator = FlowNodeId(42);

		let v1 = settings(300_000_000_000, TtlCleanupMode::Drop);
		let v2 = settings(600_000_000_000, TtlCleanupMode::Delete);

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
		// Regression for the operator-registration TTL race: the operator reads its TTL at
		// the registration transaction's version. If the settings were committed at a LATER
		// version than that snapshot, a version-pinned read returns None and the operator's
		// own tick-eviction (of its GC-immune internal state) silently never runs, leaking
		// per-row maps. The latest read (now used at registration) must still find them.
		let catalog = CatalogCache::new();
		let operator = FlowNodeId(7);
		let cfg = settings(10_000_000_000, TtlCleanupMode::Drop);

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
