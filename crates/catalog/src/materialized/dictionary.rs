// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	CommitVersion,
	interface::{DictionaryDef, DictionaryId, NamespaceId},
};

use crate::materialized::{MaterializedCatalog, MultiVersionDictionaryDef};

impl MaterializedCatalog {
	/// Find a dictionary by ID at a specific version
	pub fn find_dictionary(&self, dictionary: DictionaryId, version: CommitVersion) -> Option<DictionaryDef> {
		self.dictionaries.get(&dictionary).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a dictionary by name in a namespace at a specific version
	pub fn find_dictionary_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<DictionaryDef> {
		self.dictionaries_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let dictionary_id = *entry.value();
			self.find_dictionary(dictionary_id, version)
		})
	}

	pub fn set_dictionary(&self, id: DictionaryId, version: CommitVersion, dictionary: Option<DictionaryDef>) {
		if let Some(entry) = self.dictionaries.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				// Remove old name from index
				self.dictionaries_by_name.remove(&(pre.namespace, pre.name.clone()));
			}
		}

		let multi = self.dictionaries.get_or_insert_with(id, MultiVersionDictionaryDef::new);
		if let Some(new) = dictionary {
			self.dictionaries_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::Type;

	use super::*;

	fn create_test_dictionary(id: DictionaryId, namespace: NamespaceId, name: &str) -> DictionaryDef {
		DictionaryDef {
			id,
			namespace,
			name: name.to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		}
	}

	#[test]
	fn test_set_and_find_dictionary() {
		let catalog = MaterializedCatalog::new();
		let dict_id = DictionaryId(1);
		let namespace_id = NamespaceId(1);
		let dict = create_test_dictionary(dict_id, namespace_id, "test_dict");

		// Set dictionary at version 1
		catalog.set_dictionary(dict_id, CommitVersion(1), Some(dict.clone()));

		// Find dictionary at version 1
		let found = catalog.find_dictionary(dict_id, CommitVersion(1));
		assert_eq!(found, Some(dict.clone()));

		// Find dictionary at later version (should return same dictionary)
		let found = catalog.find_dictionary(dict_id, CommitVersion(5));
		assert_eq!(found, Some(dict));

		// Dictionary shouldn't exist at version 0
		let found = catalog.find_dictionary(dict_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_dictionary_by_name() {
		let catalog = MaterializedCatalog::new();
		let dict_id = DictionaryId(1);
		let namespace_id = NamespaceId(1);
		let dict = create_test_dictionary(dict_id, namespace_id, "named_dict");

		// Set dictionary
		catalog.set_dictionary(dict_id, CommitVersion(1), Some(dict.clone()));

		// Find by name
		let found = catalog.find_dictionary_by_name(namespace_id, "named_dict", CommitVersion(1));
		assert_eq!(found, Some(dict));

		// Shouldn't find with wrong name
		let found = catalog.find_dictionary_by_name(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		// Shouldn't find in wrong namespace
		let found = catalog.find_dictionary_by_name(NamespaceId(2), "named_dict", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_dictionary_rename() {
		let catalog = MaterializedCatalog::new();
		let dict_id = DictionaryId(1);
		let namespace_id = NamespaceId(1);

		// Create and set initial dictionary
		let dict_v1 = create_test_dictionary(dict_id, namespace_id, "old_name");
		catalog.set_dictionary(dict_id, CommitVersion(1), Some(dict_v1.clone()));

		// Verify initial state
		assert!(catalog.find_dictionary_by_name(namespace_id, "old_name", CommitVersion(1)).is_some());
		assert!(catalog.find_dictionary_by_name(namespace_id, "new_name", CommitVersion(1)).is_none());

		// Rename the dictionary
		let mut dict_v2 = dict_v1.clone();
		dict_v2.name = "new_name".to_string();
		catalog.set_dictionary(dict_id, CommitVersion(2), Some(dict_v2.clone()));

		// Old name should be gone
		assert!(catalog.find_dictionary_by_name(namespace_id, "old_name", CommitVersion(2)).is_none());

		// New name can be found
		assert_eq!(
			catalog.find_dictionary_by_name(namespace_id, "new_name", CommitVersion(2)),
			Some(dict_v2.clone())
		);

		// Historical query at version 1 should still show old name
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(1)), Some(dict_v1));

		// Current version should show new name
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(2)), Some(dict_v2));
	}

	#[test]
	fn test_dictionary_move_between_namespaces() {
		let catalog = MaterializedCatalog::new();
		let dict_id = DictionaryId(1);
		let namespace1 = NamespaceId(1);
		let namespace2 = NamespaceId(2);

		// Create dictionary in namespace1
		let dict_v1 = create_test_dictionary(dict_id, namespace1, "movable_dict");
		catalog.set_dictionary(dict_id, CommitVersion(1), Some(dict_v1.clone()));

		// Verify it's in namespace1
		assert!(catalog.find_dictionary_by_name(namespace1, "movable_dict", CommitVersion(1)).is_some());
		assert!(catalog.find_dictionary_by_name(namespace2, "movable_dict", CommitVersion(1)).is_none());

		// Move to namespace2
		let mut dict_v2 = dict_v1.clone();
		dict_v2.namespace = namespace2;
		catalog.set_dictionary(dict_id, CommitVersion(2), Some(dict_v2.clone()));

		// Should no longer be in namespace1
		assert!(catalog.find_dictionary_by_name(namespace1, "movable_dict", CommitVersion(2)).is_none());

		// Should now be in namespace2
		assert!(catalog.find_dictionary_by_name(namespace2, "movable_dict", CommitVersion(2)).is_some());
	}

	#[test]
	fn test_dictionary_deletion() {
		let catalog = MaterializedCatalog::new();
		let dict_id = DictionaryId(1);
		let namespace_id = NamespaceId(1);

		// Create and set dictionary
		let dict = create_test_dictionary(dict_id, namespace_id, "deletable_dict");
		catalog.set_dictionary(dict_id, CommitVersion(1), Some(dict.clone()));

		// Verify it exists
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(1)), Some(dict.clone()));
		assert!(catalog.find_dictionary_by_name(namespace_id, "deletable_dict", CommitVersion(1)).is_some());

		// Delete the dictionary
		catalog.set_dictionary(dict_id, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(2)), None);
		assert!(catalog.find_dictionary_by_name(namespace_id, "deletable_dict", CommitVersion(2)).is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(1)), Some(dict));
	}

	#[test]
	fn test_multiple_dictionaries_in_namespace() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);

		let dict1 = create_test_dictionary(DictionaryId(1), namespace_id, "dict1");
		let dict2 = create_test_dictionary(DictionaryId(2), namespace_id, "dict2");
		let dict3 = create_test_dictionary(DictionaryId(3), namespace_id, "dict3");

		// Set multiple dictionaries
		catalog.set_dictionary(DictionaryId(1), CommitVersion(1), Some(dict1.clone()));
		catalog.set_dictionary(DictionaryId(2), CommitVersion(1), Some(dict2.clone()));
		catalog.set_dictionary(DictionaryId(3), CommitVersion(1), Some(dict3.clone()));

		// All should be findable
		assert_eq!(catalog.find_dictionary_by_name(namespace_id, "dict1", CommitVersion(1)), Some(dict1));
		assert_eq!(catalog.find_dictionary_by_name(namespace_id, "dict2", CommitVersion(1)), Some(dict2));
		assert_eq!(catalog.find_dictionary_by_name(namespace_id, "dict3", CommitVersion(1)), Some(dict3));
	}

	#[test]
	fn test_dictionary_versioning() {
		let catalog = MaterializedCatalog::new();
		let dict_id = DictionaryId(1);
		let namespace_id = NamespaceId(1);

		// Create multiple versions
		let dict_v1 = create_test_dictionary(dict_id, namespace_id, "dict_v1");
		let mut dict_v2 = dict_v1.clone();
		dict_v2.name = "dict_v2".to_string();
		let mut dict_v3 = dict_v2.clone();
		dict_v3.name = "dict_v3".to_string();

		// Set at different versions
		catalog.set_dictionary(dict_id, CommitVersion(10), Some(dict_v1.clone()));
		catalog.set_dictionary(dict_id, CommitVersion(20), Some(dict_v2.clone()));
		catalog.set_dictionary(dict_id, CommitVersion(30), Some(dict_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(10)), Some(dict_v1.clone()));
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(15)), Some(dict_v1));
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(20)), Some(dict_v2.clone()));
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(25)), Some(dict_v2));
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(30)), Some(dict_v3.clone()));
		assert_eq!(catalog.find_dictionary(dict_id, CommitVersion(100)), Some(dict_v3));
	}
}
