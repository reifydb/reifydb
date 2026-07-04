// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::identity::{IdentityAttributeId, IdentityAttributeValue},
};
use reifydb_value::value::identity::IdentityId;

use crate::cache::{CatalogCache, MultiVersionIdentityAttributeValue};

impl CatalogCache {
	pub fn find_identity_attribute_value_at(
		&self,
		identity: IdentityId,
		attribute: IdentityAttributeId,
		version: CommitVersion,
	) -> Option<IdentityAttributeValue> {
		self.identity_attribute_values.get(&(identity, attribute)).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_identity_attribute_values_at(
		&self,
		identity: IdentityId,
		version: CommitVersion,
	) -> Vec<IdentityAttributeValue> {
		self.identity_attribute_values
			.iter()
			.filter(|entry| entry.key().0 == identity)
			.filter_map(|entry| entry.value().get(version))
			.collect()
	}

	pub fn set_identity_attribute_value(
		&self,
		identity: IdentityId,
		attribute: IdentityAttributeId,
		version: CommitVersion,
		value: Option<IdentityAttributeValue>,
	) {
		let key = (identity, attribute);
		let multi =
			self.identity_attribute_values.get_or_insert_with(key, MultiVersionIdentityAttributeValue::new);
		if let Some(new) = value {
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, interface::catalog::identity::IdentityAttributeValue};
	use reifydb_value::value::{Value, identity::IdentityId};

	use crate::cache::CatalogCache;

	fn value(identity: IdentityId, attribute: u64, value: &str) -> IdentityAttributeValue {
		IdentityAttributeValue {
			identity,
			attribute,
			value: Value::Utf8(value.to_string()),
		}
	}

	#[test]
	fn test_set_and_find_at_version() {
		let cache = CatalogCache::new();
		let alice = IdentityId::root();
		cache.set_identity_attribute_value(alice, 1, CommitVersion(5), Some(value(alice, 1, "acme")));

		assert!(cache.find_identity_attribute_value_at(alice, 1, CommitVersion(4)).is_none());
		assert_eq!(
			cache.find_identity_attribute_value_at(alice, 1, CommitVersion(5)).unwrap().value,
			Value::Utf8("acme".to_string())
		);
	}

	#[test]
	fn test_find_values_scoped_to_identity() {
		let cache = CatalogCache::new();
		let alice = IdentityId::root();
		let bob = IdentityId::system();
		cache.set_identity_attribute_value(alice, 1, CommitVersion(5), Some(value(alice, 1, "acme")));
		cache.set_identity_attribute_value(alice, 2, CommitVersion(5), Some(value(alice, 2, "pro")));
		cache.set_identity_attribute_value(bob, 1, CommitVersion(5), Some(value(bob, 1, "globex")));

		let values = cache.find_identity_attribute_values_at(alice, CommitVersion(5));
		assert_eq!(values.len(), 2);
		assert!(values.iter().all(|v| v.identity == alice));
	}

	#[test]
	fn test_remove_hides_from_later_versions() {
		let cache = CatalogCache::new();
		let alice = IdentityId::root();
		cache.set_identity_attribute_value(alice, 1, CommitVersion(5), Some(value(alice, 1, "acme")));
		cache.set_identity_attribute_value(alice, 1, CommitVersion(8), None);

		assert!(cache.find_identity_attribute_value_at(alice, 1, CommitVersion(7)).is_some());
		assert!(cache.find_identity_attribute_value_at(alice, 1, CommitVersion(8)).is_none());
	}
}
