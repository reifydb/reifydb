// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::object::ObjectId, row::RowSettings};

use crate::cache::{CatalogCache, MultiVersionRowSettings};

impl CatalogCache {
	pub fn find_row_settings_at(&self, object: ObjectId, version: CommitVersion) -> Option<RowSettings> {
		self.row_settings.get(&object).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_row_settings(&self, object: ObjectId) -> Option<RowSettings> {
		self.row_settings.get(&object).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_row_settings(&self, object: ObjectId, version: CommitVersion, settings: Option<RowSettings>) {
		let _guard = self.write_lock.lock();
		let multi = self.row_settings.get_or_insert_with(object, MultiVersionRowSettings::new);

		if let Some(new_settings) = settings {
			multi.value().insert(version, new_settings);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{interface::catalog::id::TableId, row::Ttl};
	use reifydb_value::value::duration::Duration;

	use super::*;

	fn settings(duration: Duration, announce: bool, persistent: bool) -> RowSettings {
		RowSettings {
			ttl: Some(Ttl {
				duration,
				announce,
			}),
			persistent,
		}
	}

	#[test]
	fn test_set_and_find_row_settings() {
		let catalog = CatalogCache::new();
		let object = ObjectId::Table(TableId(1));
		let config = settings(Duration::from_minutes(5).unwrap(), false, false);

		catalog.set_row_settings(object, CommitVersion(1), Some(config.clone()));

		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(1)), Some(config.clone()));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(5)), Some(config));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(0)), None);
	}

	#[test]
	fn test_row_settings_update() {
		let catalog = CatalogCache::new();
		let object = ObjectId::Table(TableId(42));

		let config_v1 = settings(Duration::from_minutes(5).unwrap(), false, true);
		let config_v2 = settings(Duration::from_minutes(10).unwrap(), true, false);

		catalog.set_row_settings(object, CommitVersion(1), Some(config_v1.clone()));
		catalog.set_row_settings(object, CommitVersion(2), Some(config_v2.clone()));

		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(1)), Some(config_v1));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(2)), Some(config_v2.clone()));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(10)), Some(config_v2));
	}

	#[test]
	fn test_row_settings_deletion() {
		let catalog = CatalogCache::new();
		let object = ObjectId::Table(TableId(99));
		let config = settings(Duration::from_minutes(5).unwrap(), false, true);

		catalog.set_row_settings(object, CommitVersion(1), Some(config.clone()));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(1)), Some(config.clone()));

		catalog.set_row_settings(object, CommitVersion(2), None);
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(2)), None);
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(1)), Some(config));
	}

	#[test]
	fn test_row_settings_versioning() {
		let catalog = CatalogCache::new();
		let object = ObjectId::Table(TableId(100));

		let config_v1 = settings(Duration::from_minutes(1).unwrap(), false, true);
		let config_v2 = settings(Duration::from_minutes(5).unwrap(), true, false);
		let config_v3 = settings(Duration::from_days(1).unwrap(), false, true);

		catalog.set_row_settings(object, CommitVersion(10), Some(config_v1.clone()));
		catalog.set_row_settings(object, CommitVersion(20), Some(config_v2.clone()));
		catalog.set_row_settings(object, CommitVersion(30), Some(config_v3.clone()));

		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(5)), None);
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(10)), Some(config_v1.clone()));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(15)), Some(config_v1));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(20)), Some(config_v2.clone()));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(25)), Some(config_v2));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(30)), Some(config_v3.clone()));
		assert_eq!(catalog.find_row_settings_at(object, CommitVersion(100)), Some(config_v3));
	}
}
