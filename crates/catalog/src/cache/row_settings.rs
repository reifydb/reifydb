// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::shape::ShapeId, row::RowSettings};

use crate::cache::{CatalogCache, MultiVersionRowSettings};

impl CatalogCache {
	pub fn find_row_settings_at(&self, shape: ShapeId, version: CommitVersion) -> Option<RowSettings> {
		self.row_settings.get(&shape).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_row_settings(&self, shape: ShapeId) -> Option<RowSettings> {
		self.row_settings.get(&shape).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_row_settings(&self, shape: ShapeId, version: CommitVersion, settings: Option<RowSettings>) {
		let multi = self.row_settings.get_or_insert_with(shape, MultiVersionRowSettings::new);

		if let Some(new_settings) = settings {
			multi.value().insert(version, new_settings);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		row::{Ttl, TtlCleanupMode},
	};

	use super::*;

	fn settings(duration_nanos: u64, cleanup_mode: TtlCleanupMode, persistent: bool) -> RowSettings {
		RowSettings {
			ttl: Some(Ttl {
				duration_nanos,
				cleanup_mode,
			}),
			persistent,
		}
	}

	#[test]
	fn test_set_and_find_row_settings() {
		let catalog = CatalogCache::new();
		let shape = ShapeId::Table(TableId(1));
		let config = settings(300_000_000_000, TtlCleanupMode::Drop, false);

		catalog.set_row_settings(shape, CommitVersion(1), Some(config.clone()));

		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(1)), Some(config.clone()));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(5)), Some(config));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(0)), None);
	}

	#[test]
	fn test_row_settings_update() {
		let catalog = CatalogCache::new();
		let shape = ShapeId::Table(TableId(42));

		let config_v1 = settings(300_000_000_000, TtlCleanupMode::Drop, true);
		let config_v2 = settings(600_000_000_000, TtlCleanupMode::Delete, false);

		catalog.set_row_settings(shape, CommitVersion(1), Some(config_v1.clone()));
		catalog.set_row_settings(shape, CommitVersion(2), Some(config_v2.clone()));

		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(1)), Some(config_v1));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(2)), Some(config_v2.clone()));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(10)), Some(config_v2));
	}

	#[test]
	fn test_row_settings_deletion() {
		let catalog = CatalogCache::new();
		let shape = ShapeId::Table(TableId(99));
		let config = settings(300_000_000_000, TtlCleanupMode::Drop, true);

		catalog.set_row_settings(shape, CommitVersion(1), Some(config.clone()));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(1)), Some(config.clone()));

		catalog.set_row_settings(shape, CommitVersion(2), None);
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(2)), None);
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(1)), Some(config));
	}

	#[test]
	fn test_row_settings_versioning() {
		let catalog = CatalogCache::new();
		let shape = ShapeId::Table(TableId(100));

		let config_v1 = settings(60_000_000_000, TtlCleanupMode::Drop, true);
		let config_v2 = settings(300_000_000_000, TtlCleanupMode::Delete, false);
		let config_v3 = settings(86_400_000_000_000, TtlCleanupMode::Drop, true);

		catalog.set_row_settings(shape, CommitVersion(10), Some(config_v1.clone()));
		catalog.set_row_settings(shape, CommitVersion(20), Some(config_v2.clone()));
		catalog.set_row_settings(shape, CommitVersion(30), Some(config_v3.clone()));

		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(5)), None);
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(10)), Some(config_v1.clone()));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(15)), Some(config_v1));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(20)), Some(config_v2.clone()));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(25)), Some(config_v2));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(30)), Some(config_v3.clone()));
		assert_eq!(catalog.find_row_settings_at(shape, CommitVersion(100)), Some(config_v3));
	}
}
