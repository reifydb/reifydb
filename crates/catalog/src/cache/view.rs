// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, ViewId},
		view::View,
	},
};

use crate::cache::{CatalogCache, MultiVersionView};

impl CatalogCache {
	pub fn find_view_at(&self, view: ViewId, version: CommitVersion) -> Option<View> {
		self.views.get(&view).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_view_by_name_at(&self, namespace: NamespaceId, name: &str, version: CommitVersion) -> Option<View> {
		self.views_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let view_id = *entry.value();
			self.find_view_at(view_id, version)
		})
	}

	pub fn find_view(&self, view: ViewId) -> Option<View> {
		self.views.get(&view).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_view_by_name(&self, namespace: NamespaceId, name: &str) -> Option<View> {
		self.views_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let view_id = *entry.value();
			self.find_view(view_id)
		})
	}

	pub fn set_view(&self, id: ViewId, version: CommitVersion, view: Option<View>) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.views.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.views_by_name.remove(&(pre.namespace(), pre.name().to_string()));
		}

		let multi = self.views.get_or_insert_with(id, MultiVersionView::new);
		if let Some(new) = view {
			self.views_by_name.insert((new.namespace(), new.name().to_string()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		column::{Column, ColumnIndex},
		id::ColumnId,
		view::{TableView, ViewKind},
	};
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use super::*;

	fn create_test_view(id: ViewId, namespace: NamespaceId, name: &str) -> View {
		View::Table(TableView {
			id,
			namespace,
			name: name.to_string(),
			kind: ViewKind::Deferred,
			columns: vec![
				Column {
					id: ColumnId(1),
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Int1),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: ColumnId(2),
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
			],
			primary_key: None,
			partition_by: vec![],
			sort: vec![],
		})
	}

	#[test]
	fn test_set_and_find_view() {
		let catalog = CatalogCache::new();
		let view_id = ViewId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let view = create_test_view(view_id, namespace_id, "test_view");

		catalog.set_view(view_id, CommitVersion(1), Some(view.clone()));

		let found = catalog.find_view_at(view_id, CommitVersion(1));
		assert_eq!(found, Some(view.clone()));

		let found = catalog.find_view_at(view_id, CommitVersion(5));
		assert_eq!(found, Some(view));

		let found = catalog.find_view_at(view_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_view_by_name() {
		let catalog = CatalogCache::new();
		let view_id = ViewId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let view = create_test_view(view_id, namespace_id, "named_view");

		catalog.set_view(view_id, CommitVersion(1), Some(view.clone()));

		let found = catalog.find_view_by_name_at(namespace_id, "named_view", CommitVersion(1));
		assert_eq!(found, Some(view));

		let found = catalog.find_view_by_name_at(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		let found = catalog.find_view_by_name_at(NamespaceId::DEFAULT, "named_view", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_view_rename() {
		let catalog = CatalogCache::new();
		let view_id = ViewId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let view_v1 = create_test_view(view_id, namespace_id, "old_name");
		catalog.set_view(view_id, CommitVersion(1), Some(view_v1.clone()));

		assert!(catalog.find_view_by_name_at(namespace_id, "old_name", CommitVersion(1)).is_some());
		assert!(catalog.find_view_by_name_at(namespace_id, "new_name", CommitVersion(1)).is_none());

		let mut view_v2 = view_v1.clone();
		view_v2.set_name("new_name".to_string());
		catalog.set_view(view_id, CommitVersion(2), Some(view_v2.clone()));

		assert!(catalog.find_view_by_name_at(namespace_id, "old_name", CommitVersion(2)).is_none());

		assert_eq!(
			catalog.find_view_by_name_at(namespace_id, "new_name", CommitVersion(2)),
			Some(view_v2.clone())
		);

		assert_eq!(catalog.find_view_at(view_id, CommitVersion(1)), Some(view_v1));

		assert_eq!(catalog.find_view_at(view_id, CommitVersion(2)), Some(view_v2));
	}

	#[test]
	fn test_view_move_between_namespaces() {
		let catalog = CatalogCache::new();
		let view_id = ViewId(1);
		let namespace1 = NamespaceId::SYSTEM;
		let namespace2 = NamespaceId::DEFAULT;

		let view_v1 = create_test_view(view_id, namespace1, "movable_view");
		catalog.set_view(view_id, CommitVersion(1), Some(view_v1.clone()));

		assert!(catalog.find_view_by_name_at(namespace1, "movable_view", CommitVersion(1)).is_some());
		assert!(catalog.find_view_by_name_at(namespace2, "movable_view", CommitVersion(1)).is_none());

		let mut view_v2 = view_v1.clone();
		view_v2.set_namespace(namespace2);
		catalog.set_view(view_id, CommitVersion(2), Some(view_v2.clone()));

		assert!(catalog.find_view_by_name_at(namespace1, "movable_view", CommitVersion(2)).is_none());

		assert!(catalog.find_view_by_name_at(namespace2, "movable_view", CommitVersion(2)).is_some());
	}

	#[test]
	fn test_view_deletion() {
		let catalog = CatalogCache::new();
		let view_id = ViewId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let view = create_test_view(view_id, namespace_id, "deletable_view");
		catalog.set_view(view_id, CommitVersion(1), Some(view.clone()));

		assert_eq!(catalog.find_view_at(view_id, CommitVersion(1)), Some(view.clone()));
		assert!(catalog.find_view_by_name_at(namespace_id, "deletable_view", CommitVersion(1)).is_some());

		catalog.set_view(view_id, CommitVersion(2), None);

		assert_eq!(catalog.find_view_at(view_id, CommitVersion(2)), None);
		assert!(catalog.find_view_by_name_at(namespace_id, "deletable_view", CommitVersion(2)).is_none());

		assert_eq!(catalog.find_view_at(view_id, CommitVersion(1)), Some(view));
	}

	#[test]
	fn test_multiple_views_in_namespace() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		let view1 = create_test_view(ViewId(1), namespace_id, "view1");
		let view2 = create_test_view(ViewId(2), namespace_id, "view2");
		let view3 = create_test_view(ViewId(3), namespace_id, "view3");

		catalog.set_view(ViewId(1), CommitVersion(1), Some(view1.clone()));
		catalog.set_view(ViewId(2), CommitVersion(1), Some(view2.clone()));
		catalog.set_view(ViewId(3), CommitVersion(1), Some(view3.clone()));

		assert_eq!(catalog.find_view_by_name_at(namespace_id, "view1", CommitVersion(1)), Some(view1));
		assert_eq!(catalog.find_view_by_name_at(namespace_id, "view2", CommitVersion(1)), Some(view2));
		assert_eq!(catalog.find_view_by_name_at(namespace_id, "view3", CommitVersion(1)), Some(view3));
	}

	#[test]
	fn test_view_versioning() {
		let catalog = CatalogCache::new();
		let view_id = ViewId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let view_v1 = create_test_view(view_id, namespace_id, "view_v1");
		let mut view_v2 = view_v1.clone();
		view_v2.set_name("view_v2".to_string());
		let mut view_v3 = view_v2.clone();
		view_v3.set_name("view_v3".to_string());

		catalog.set_view(view_id, CommitVersion(10), Some(view_v1.clone()));
		catalog.set_view(view_id, CommitVersion(20), Some(view_v2.clone()));
		catalog.set_view(view_id, CommitVersion(30), Some(view_v3.clone()));

		assert_eq!(catalog.find_view_at(view_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_view_at(view_id, CommitVersion(10)), Some(view_v1.clone()));
		assert_eq!(catalog.find_view_at(view_id, CommitVersion(15)), Some(view_v1));
		assert_eq!(catalog.find_view_at(view_id, CommitVersion(20)), Some(view_v2.clone()));
		assert_eq!(catalog.find_view_at(view_id, CommitVersion(25)), Some(view_v2));
		assert_eq!(catalog.find_view_at(view_id, CommitVersion(30)), Some(view_v3.clone()));
		assert_eq!(catalog.find_view_at(view_id, CommitVersion(100)), Some(view_v3));
	}

	#[test]
	fn test_find_latest_view() {
		let catalog = CatalogCache::new();
		let view_id = ViewId(1);
		let namespace_id = NamespaceId::SYSTEM;

		assert_eq!(catalog.find_view(view_id), None);

		let view_v1 = create_test_view(view_id, namespace_id, "view_v1");
		let mut view_v2 = view_v1.clone();
		view_v2.set_name("view_v2".to_string());

		catalog.set_view(view_id, CommitVersion(10), Some(view_v1));
		catalog.set_view(view_id, CommitVersion(20), Some(view_v2.clone()));

		assert_eq!(catalog.find_view(view_id), Some(view_v2));
	}

	#[test]
	fn test_find_latest_view_deleted() {
		let catalog = CatalogCache::new();
		let view_id = ViewId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let view = create_test_view(view_id, namespace_id, "test_view");
		catalog.set_view(view_id, CommitVersion(10), Some(view));

		catalog.set_view(view_id, CommitVersion(20), None);

		assert_eq!(catalog.find_view(view_id), None);
	}

	#[test]
	fn test_find_latest_view_by_name() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;
		let view_id = ViewId(1);

		assert_eq!(catalog.find_view_by_name(namespace_id, "test_view"), None);

		let view_v1 = create_test_view(view_id, namespace_id, "test_view");
		let mut view_v2 = view_v1.clone();
		view_v2.set_name("renamed_view".to_string());

		catalog.set_view(view_id, CommitVersion(10), Some(view_v1));
		catalog.set_view(view_id, CommitVersion(20), Some(view_v2.clone()));

		assert_eq!(catalog.find_view_by_name(namespace_id, "test_view"), None);

		assert_eq!(catalog.find_view_by_name(namespace_id, "renamed_view"), Some(view_v2));
	}
}
