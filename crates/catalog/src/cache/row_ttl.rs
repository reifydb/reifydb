// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::shape::ShapeId, row::Ttl};

use crate::cache::{CatalogCache, MultiVersionRowTtl};

impl CatalogCache {
	pub fn find_row_ttl_at(&self, shape: ShapeId, version: CommitVersion) -> Option<Ttl> {
		self.row_ttls.get(&shape).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_row_ttl(&self, shape: ShapeId) -> Option<Ttl> {
		self.row_ttls.get(&shape).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_row_ttl(&self, shape: ShapeId, version: CommitVersion, config: Option<Ttl>) {
		let multi = self.row_ttls.get_or_insert_with(shape, MultiVersionRowTtl::new);

		if let Some(new_config) = config {
			multi.value().insert(version, new_config);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		row::{TtlAnchor, TtlCleanupMode},
	};

	use super::*;

	#[test]
	fn test_set_and_find_row_ttl() {
		let catalog = CatalogCache::new();
		let shape = ShapeId::Table(TableId(1));
		let config = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		catalog.set_row_ttl(shape, CommitVersion(1), Some(config.clone()));

		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(1)), Some(config.clone()));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(5)), Some(config));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(0)), None);
	}

	#[test]
	fn test_row_ttl_update() {
		let catalog = CatalogCache::new();
		let shape = ShapeId::Table(TableId(42));

		let config_v1 = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};
		let config_v2 = Ttl {
			duration_nanos: 600_000_000_000,
			anchor: TtlAnchor::Updated,
			cleanup_mode: TtlCleanupMode::Delete,
		};

		catalog.set_row_ttl(shape, CommitVersion(1), Some(config_v1.clone()));
		catalog.set_row_ttl(shape, CommitVersion(2), Some(config_v2.clone()));

		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(1)), Some(config_v1));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(2)), Some(config_v2.clone()));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(10)), Some(config_v2));
	}

	#[test]
	fn test_row_ttl_deletion() {
		let catalog = CatalogCache::new();
		let shape = ShapeId::Table(TableId(99));
		let config = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		catalog.set_row_ttl(shape, CommitVersion(1), Some(config.clone()));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(1)), Some(config.clone()));

		catalog.set_row_ttl(shape, CommitVersion(2), None);
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(2)), None);
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(1)), Some(config));
	}

	#[test]
	fn test_row_ttl_versioning() {
		let catalog = CatalogCache::new();
		let shape = ShapeId::Table(TableId(100));

		let config_v1 = Ttl {
			duration_nanos: 60_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};
		let config_v2 = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Updated,
			cleanup_mode: TtlCleanupMode::Delete,
		};
		let config_v3 = Ttl {
			duration_nanos: 86_400_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		catalog.set_row_ttl(shape, CommitVersion(10), Some(config_v1.clone()));
		catalog.set_row_ttl(shape, CommitVersion(20), Some(config_v2.clone()));
		catalog.set_row_ttl(shape, CommitVersion(30), Some(config_v3.clone()));

		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(5)), None);
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(10)), Some(config_v1.clone()));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(15)), Some(config_v1));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(20)), Some(config_v2.clone()));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(25)), Some(config_v2));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(30)), Some(config_v3.clone()));
		assert_eq!(catalog.find_row_ttl_at(shape, CommitVersion(100)), Some(config_v3));
	}
}
