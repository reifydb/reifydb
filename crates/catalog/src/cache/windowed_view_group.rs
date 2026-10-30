// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, WindowedViewGroupId},
		windowed_view_group::WindowedViewGroup,
	},
};

use crate::cache::{CatalogCache, MultiVersionWindowedViewGroup};

impl CatalogCache {
	pub fn find_windowed_view_group_at(
		&self,
		group: WindowedViewGroupId,
		version: CommitVersion,
	) -> Option<WindowedViewGroup> {
		self.windowed_view_groups.get(&group).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_windowed_view_group_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<WindowedViewGroup> {
		self.windowed_view_groups_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let group_id = *entry.value();
			self.find_windowed_view_group_at(group_id, version)
		})
	}

	pub fn find_windowed_view_group(&self, group: WindowedViewGroupId) -> Option<WindowedViewGroup> {
		self.windowed_view_groups.get(&group).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_windowed_view_group_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> Option<WindowedViewGroup> {
		self.windowed_view_groups_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let group_id = *entry.value();
			self.find_windowed_view_group(group_id)
		})
	}

	pub fn set_windowed_view_group(
		&self,
		id: WindowedViewGroupId,
		version: CommitVersion,
		group: Option<WindowedViewGroup>,
	) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.windowed_view_groups.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.windowed_view_groups_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.windowed_view_groups.get_or_insert_with(id, MultiVersionWindowedViewGroup::new);
		if let Some(new) = group {
			self.windowed_view_groups_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{flow::FlowId, id::ViewId};

	use super::*;

	fn create_test_group(id: WindowedViewGroupId, namespace: NamespaceId, name: &str) -> WindowedViewGroup {
		WindowedViewGroup {
			id,
			namespace,
			name: name.to_string(),
			flow: FlowId(1),
			members: vec![ViewId(1), ViewId(2)],
		}
	}

	#[test]
	fn test_set_and_find_windowed_view_group() {
		let catalog = CatalogCache::new();
		let group_id = WindowedViewGroupId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let group = create_test_group(group_id, namespace_id, "test_group");

		catalog.set_windowed_view_group(group_id, CommitVersion(1), Some(group.clone()));

		let found = catalog.find_windowed_view_group_at(group_id, CommitVersion(1));
		assert_eq!(found, Some(group.clone()));

		let found = catalog.find_windowed_view_group_at(group_id, CommitVersion(5));
		assert_eq!(found, Some(group));

		let found = catalog.find_windowed_view_group_at(group_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_windowed_view_group_by_name() {
		let catalog = CatalogCache::new();
		let group_id = WindowedViewGroupId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let group = create_test_group(group_id, namespace_id, "named_group");

		catalog.set_windowed_view_group(group_id, CommitVersion(1), Some(group.clone()));

		let found = catalog.find_windowed_view_group_by_name_at(namespace_id, "named_group", CommitVersion(1));
		assert_eq!(found, Some(group));

		let found = catalog.find_windowed_view_group_by_name_at(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		let found = catalog.find_windowed_view_group_by_name_at(
			NamespaceId::DEFAULT,
			"named_group",
			CommitVersion(1),
		);
		assert_eq!(found, None);
	}

	#[test]
	fn test_windowed_view_group_deletion() {
		let catalog = CatalogCache::new();
		let group_id = WindowedViewGroupId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let group = create_test_group(group_id, namespace_id, "deletable_group");
		catalog.set_windowed_view_group(group_id, CommitVersion(1), Some(group.clone()));

		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(1)), Some(group.clone()));
		assert!(catalog
			.find_windowed_view_group_by_name_at(namespace_id, "deletable_group", CommitVersion(1))
			.is_some());

		catalog.set_windowed_view_group(group_id, CommitVersion(2), None);

		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(2)), None);
		assert!(catalog
			.find_windowed_view_group_by_name_at(namespace_id, "deletable_group", CommitVersion(2))
			.is_none());

		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(1)), Some(group));
	}

	#[test]
	fn test_multiple_windowed_view_groups_in_namespace() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		let group1 = create_test_group(WindowedViewGroupId(1), namespace_id, "group1");
		let group2 = create_test_group(WindowedViewGroupId(2), namespace_id, "group2");
		let group3 = create_test_group(WindowedViewGroupId(3), namespace_id, "group3");

		catalog.set_windowed_view_group(WindowedViewGroupId(1), CommitVersion(1), Some(group1.clone()));
		catalog.set_windowed_view_group(WindowedViewGroupId(2), CommitVersion(1), Some(group2.clone()));
		catalog.set_windowed_view_group(WindowedViewGroupId(3), CommitVersion(1), Some(group3.clone()));

		assert_eq!(
			catalog.find_windowed_view_group_by_name_at(namespace_id, "group1", CommitVersion(1)),
			Some(group1)
		);
		assert_eq!(
			catalog.find_windowed_view_group_by_name_at(namespace_id, "group2", CommitVersion(1)),
			Some(group2)
		);
		assert_eq!(
			catalog.find_windowed_view_group_by_name_at(namespace_id, "group3", CommitVersion(1)),
			Some(group3)
		);
	}

	#[test]
	fn test_windowed_view_group_versioning() {
		let catalog = CatalogCache::new();
		let group_id = WindowedViewGroupId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let group_v1 = create_test_group(group_id, namespace_id, "group_v1");
		let mut group_v2 = group_v1.clone();
		group_v2.name = "group_v2".to_string();
		let mut group_v3 = group_v2.clone();
		group_v3.name = "group_v3".to_string();

		catalog.set_windowed_view_group(group_id, CommitVersion(10), Some(group_v1.clone()));
		catalog.set_windowed_view_group(group_id, CommitVersion(20), Some(group_v2.clone()));
		catalog.set_windowed_view_group(group_id, CommitVersion(30), Some(group_v3.clone()));

		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(10)), Some(group_v1.clone()));
		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(15)), Some(group_v1));
		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(20)), Some(group_v2.clone()));
		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(25)), Some(group_v2));
		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(30)), Some(group_v3.clone()));
		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(100)), Some(group_v3));
	}

	#[test]
	fn test_windowed_view_group_with_fifteen_members_keeps_order() {
		// The cache must hand back members in declaration order, never sorted or deduplicated.
		let catalog = CatalogCache::new();
		let group_id = WindowedViewGroupId(1);
		let members: Vec<ViewId> =
			[9, 3, 15, 1, 12, 7, 2, 14, 5, 11, 4, 13, 6, 10, 8].into_iter().map(ViewId).collect();
		let group = WindowedViewGroup {
			id: group_id,
			namespace: NamespaceId::SYSTEM,
			name: "wide_group".to_string(),
			flow: FlowId(1),
			members: members.clone(),
		};

		catalog.set_windowed_view_group(group_id, CommitVersion(1), Some(group));

		assert_eq!(catalog.find_windowed_view_group_at(group_id, CommitVersion(1)).unwrap().members, members);
	}
}
