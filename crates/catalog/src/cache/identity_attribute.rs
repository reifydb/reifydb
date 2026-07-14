// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::identity::{IdentityAttribute, IdentityAttributeId},
};

use crate::cache::{CatalogCache, MultiVersionIdentityAttribute};

impl CatalogCache {
	pub fn find_identity_attribute_at(
		&self,
		id: IdentityAttributeId,
		version: CommitVersion,
	) -> Option<IdentityAttribute> {
		self.identity_attributes.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_identity_attribute_by_name_at(
		&self,
		name: &str,
		version: CommitVersion,
	) -> Option<IdentityAttribute> {
		self.identity_attributes_by_name.get(name).and_then(|entry| {
			let attribute_id = *entry.value();
			self.find_identity_attribute_at(attribute_id, version)
		})
	}

	pub fn list_all_identity_attributes_at(&self, version: CommitVersion) -> Vec<IdentityAttribute> {
		self.identity_attributes.iter().filter_map(|entry| entry.value().get(version)).collect()
	}

	pub fn set_identity_attribute(
		&self,
		id: IdentityAttributeId,
		version: CommitVersion,
		attribute: Option<IdentityAttribute>,
	) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.identity_attributes.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.identity_attributes_by_name.remove(&pre.name);
		}

		let multi = self.identity_attributes.get_or_insert_with(id, MultiVersionIdentityAttribute::new);
		if let Some(new) = attribute {
			self.identity_attributes_by_name.insert(new.name.clone(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, interface::catalog::identity::IdentityAttribute};
	use reifydb_value::value::value_type::ValueType;

	use crate::cache::CatalogCache;

	fn attribute(id: u64, name: &str) -> IdentityAttribute {
		IdentityAttribute {
			id,
			name: name.to_string(),
			value_type: ValueType::Utf8,
		}
	}

	#[test]
	fn test_set_and_find_at_version() {
		let cache = CatalogCache::new();
		cache.set_identity_attribute(1, CommitVersion(5), Some(attribute(1, "org_id")));

		assert!(cache.find_identity_attribute_at(1, CommitVersion(4)).is_none());
		assert_eq!(cache.find_identity_attribute_at(1, CommitVersion(5)).unwrap().name, "org_id");
		assert_eq!(cache.find_identity_attribute_at(1, CommitVersion(9)).unwrap().name, "org_id");
	}

	#[test]
	fn test_find_by_name_at() {
		let cache = CatalogCache::new();
		cache.set_identity_attribute(1, CommitVersion(5), Some(attribute(1, "org_id")));

		assert!(cache.find_identity_attribute_by_name_at("org_id", CommitVersion(4)).is_none());
		assert_eq!(cache.find_identity_attribute_by_name_at("org_id", CommitVersion(5)).unwrap().id, 1);
		assert!(cache.find_identity_attribute_by_name_at("tier", CommitVersion(5)).is_none());
	}

	#[test]
	fn test_remove_hides_from_later_versions() {
		let cache = CatalogCache::new();
		cache.set_identity_attribute(1, CommitVersion(5), Some(attribute(1, "org_id")));
		cache.set_identity_attribute(1, CommitVersion(8), None);

		assert!(cache.find_identity_attribute_at(1, CommitVersion(7)).is_some());
		assert!(cache.find_identity_attribute_at(1, CommitVersion(8)).is_none());
	}

	#[test]
	fn test_list_all_at_version() {
		let cache = CatalogCache::new();
		cache.set_identity_attribute(1, CommitVersion(5), Some(attribute(1, "org_id")));
		cache.set_identity_attribute(2, CommitVersion(7), Some(attribute(2, "tier")));

		assert_eq!(cache.list_all_identity_attributes_at(CommitVersion(5)).len(), 1);
		assert_eq!(cache.list_all_identity_attributes_at(CommitVersion(7)).len(), 2);
	}
}
