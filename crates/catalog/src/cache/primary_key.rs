// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::PrimaryKeyId, key::PrimaryKey, object::ObjectId},
};

use crate::cache::{CatalogCache, MultiVersionPrimaryKey};

impl CatalogCache {
	pub fn find_primary_key_at(&self, primary_key_id: PrimaryKeyId, version: CommitVersion) -> Option<PrimaryKey> {
		self.primary_keys.get(&primary_key_id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_primary_key(&self, primary_key_id: PrimaryKeyId) -> Option<PrimaryKey> {
		self.primary_keys.get(&primary_key_id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_primary_key(&self, id: PrimaryKeyId, version: CommitVersion, primary_key: Option<PrimaryKey>) {
		let _guard = self.write_lock.lock();
		let multi = self.primary_keys.get_or_insert_with(id, MultiVersionPrimaryKey::new);
		if let Some(new) = primary_key {
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}

	pub fn set_primary_key_object(&self, object: ObjectId, primary_key_id: PrimaryKeyId) {
		let _guard = self.write_lock.lock();
		self.primary_keys_by_object.insert(object, primary_key_id);
	}

	pub fn find_primary_key_id_by_object(&self, object: ObjectId) -> Option<PrimaryKeyId> {
		self.primary_keys_by_object.get(&object).map(|entry| *entry.value())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		column::{Column, ColumnIndex},
		id::{ColumnId, PrimaryKeyId},
	};
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use super::*;
	use crate::cache::CatalogCache;

	fn create_test_primary_key(id: PrimaryKeyId) -> PrimaryKey {
		PrimaryKey {
			id,
			columns: vec![Column {
				id: ColumnId(1),
				name: "id".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Int4),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			}],
		}
	}

	#[test]
	fn test_set_and_find_primary_key() {
		let catalog = CatalogCache::new();
		let pk_id = PrimaryKeyId(1);
		let primary_key = create_test_primary_key(pk_id);

		catalog.set_primary_key(pk_id, CommitVersion(1), Some(primary_key.clone()));

		let found = catalog.find_primary_key_at(pk_id, CommitVersion(1));
		assert_eq!(found, Some(primary_key.clone()));

		let found = catalog.find_primary_key_at(pk_id, CommitVersion(5));
		assert_eq!(found, Some(primary_key));

		let found = catalog.find_primary_key_at(pk_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_primary_key_update() {
		let catalog = CatalogCache::new();
		let pk_id = PrimaryKeyId(1);

		let pk_v1 = create_test_primary_key(pk_id);
		catalog.set_primary_key(pk_id, CommitVersion(1), Some(pk_v1.clone()));

		let mut pk_v2 = pk_v1.clone();
		pk_v2.columns.push(Column {
			id: ColumnId(2),
			name: "name".to_string(),
			constraint: TypeConstraint::unconstrained(ValueType::Utf8),
			properties: vec![],
			index: ColumnIndex(1),
			auto_increment: false,
			dictionary_id: None,
		});
		catalog.set_primary_key(pk_id, CommitVersion(2), Some(pk_v2.clone()));

		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(1)).unwrap().columns.len(), 1);

		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(2)).unwrap().columns.len(), 2);
	}

	#[test]
	fn test_primary_key_deletion() {
		let catalog = CatalogCache::new();
		let pk_id = PrimaryKeyId(1);
		let primary_key = create_test_primary_key(pk_id);

		catalog.set_primary_key(pk_id, CommitVersion(1), Some(primary_key.clone()));

		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(1)), Some(primary_key.clone()));

		catalog.set_primary_key(pk_id, CommitVersion(2), None);

		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(2)), None);

		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(1)), Some(primary_key));
	}

	#[test]
	fn test_primary_key_versioning() {
		let catalog = CatalogCache::new();
		let pk_id = PrimaryKeyId(1);

		let pk_v1 = create_test_primary_key(pk_id);
		let mut pk_v2 = pk_v1.clone();
		pk_v2.columns[0].name = "pk_id".to_string();
		let mut pk_v3 = pk_v2.clone();
		pk_v3.columns[0].constraint = TypeConstraint::unconstrained(ValueType::Int8);

		catalog.set_primary_key(pk_id, CommitVersion(10), Some(pk_v1.clone()));
		catalog.set_primary_key(pk_id, CommitVersion(20), Some(pk_v2.clone()));
		catalog.set_primary_key(pk_id, CommitVersion(30), Some(pk_v3.clone()));

		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(10)), Some(pk_v1.clone()));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(15)), Some(pk_v1));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(20)), Some(pk_v2.clone()));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(25)), Some(pk_v2));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(30)), Some(pk_v3.clone()));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(100)), Some(pk_v3));
	}
}
