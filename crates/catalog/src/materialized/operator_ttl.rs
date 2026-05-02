// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId, row::Ttl};

use crate::materialized::{MaterializedCatalog, MultiVersionRowTtl};

impl MaterializedCatalog {
	/// Find a per-operator TTL config at a specific version.
	pub fn find_operator_ttl_at(&self, node: FlowNodeId, version: CommitVersion) -> Option<Ttl> {
		self.operator_ttls.get(&node).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a per-operator TTL config (latest version).
	pub fn find_operator_ttl(&self, node: FlowNodeId) -> Option<Ttl> {
		self.operator_ttls.get(&node).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Set a per-operator TTL config at a specific version.
	pub fn set_operator_ttl(&self, node: FlowNodeId, version: CommitVersion, config: Option<Ttl>) {
		let multi = self.operator_ttls.get_or_insert_with(node, MultiVersionRowTtl::new);

		if let Some(new_config) = config {
			multi.value().insert(version, new_config);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::row::{TtlAnchor, TtlCleanupMode};

	use super::*;

	#[test]
	fn test_set_and_find_operator_ttl() {
		let catalog = MaterializedCatalog::new();
		let node = FlowNodeId(1);
		let config = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		catalog.set_operator_ttl(node, CommitVersion(1), Some(config.clone()));

		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(1)), Some(config.clone()));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(5)), Some(config.clone()));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(0)), None);
		assert_eq!(catalog.find_operator_ttl(node), Some(config));
	}

	#[test]
	fn test_operator_ttl_update() {
		let catalog = MaterializedCatalog::new();
		let node = FlowNodeId(42);

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

		catalog.set_operator_ttl(node, CommitVersion(1), Some(config_v1.clone()));
		catalog.set_operator_ttl(node, CommitVersion(2), Some(config_v2.clone()));

		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(1)), Some(config_v1));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(2)), Some(config_v2.clone()));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(10)), Some(config_v2));
	}

	#[test]
	fn test_operator_ttl_deletion() {
		let catalog = MaterializedCatalog::new();
		let node = FlowNodeId(99);
		let config = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		catalog.set_operator_ttl(node, CommitVersion(1), Some(config.clone()));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(1)), Some(config.clone()));

		catalog.set_operator_ttl(node, CommitVersion(2), None);
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(2)), None);
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(1)), Some(config));
	}

	#[test]
	fn test_operator_ttl_versioning() {
		let catalog = MaterializedCatalog::new();
		let node = FlowNodeId(100);

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

		catalog.set_operator_ttl(node, CommitVersion(10), Some(config_v1.clone()));
		catalog.set_operator_ttl(node, CommitVersion(20), Some(config_v2.clone()));
		catalog.set_operator_ttl(node, CommitVersion(30), Some(config_v3.clone()));

		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(5)), None);
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(10)), Some(config_v1.clone()));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(15)), Some(config_v1));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(20)), Some(config_v2.clone()));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(25)), Some(config_v2));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(30)), Some(config_v3.clone()));
		assert_eq!(catalog.find_operator_ttl_at(node, CommitVersion(100)), Some(config_v3));
	}

	#[test]
	fn test_operator_ttl_per_node_isolation() {
		let catalog = MaterializedCatalog::new();
		let node_a = FlowNodeId(1);
		let node_b = FlowNodeId(2);

		let cfg_a = Ttl {
			duration_nanos: 5_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};
		let cfg_b = Ttl {
			duration_nanos: 3_600_000_000_000,
			anchor: TtlAnchor::Updated,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		catalog.set_operator_ttl(node_a, CommitVersion(1), Some(cfg_a.clone()));
		catalog.set_operator_ttl(node_b, CommitVersion(1), Some(cfg_b.clone()));

		assert_eq!(catalog.find_operator_ttl(node_a), Some(cfg_a));
		assert_eq!(catalog.find_operator_ttl(node_b), Some(cfg_b));

		catalog.set_operator_ttl(node_a, CommitVersion(2), None);
		assert_eq!(catalog.find_operator_ttl(node_a), None);
		assert!(catalog.find_operator_ttl(node_b).is_some());
	}
}
