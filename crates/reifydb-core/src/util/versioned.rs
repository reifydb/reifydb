// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use crossbeam_skiplist::SkipMap;

use crate::Version;

/// A thread-safe container for versioned values.
///
/// This structure maintains multiple versions of a definition, allowing
/// for point-in-time queries and concurrent access patterns.
#[derive(Debug)]
pub struct VersionedContainer<T: Debug + Clone + Send + Sync + 'static> {
	inner: Arc<RwLock<VersionedDefInner<T>>>,
}

#[derive(Debug)]
struct VersionedDefInner<T: Debug + Clone + Send + Sync + 'static> {
	versions: SkipMap<Version, Option<T>>,
}

impl<T: Debug + Clone + Send + Sync + 'static> VersionedContainer<T> {
	/// Creates a new empty versioned definition container.
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(VersionedDefInner {
				versions: SkipMap::new(),
			})),
		}
	}

	/// Creates a new versioned definition with initial data.
	pub fn with_initial(version: Version, def: Option<T>) -> Self {
		let versioned = Self::new();
		versioned.insert(version, def);
		versioned
	}

	/// Inserts a definition at a specific version.
	///
	/// Returns the previous definition at this version if one existed.
	pub fn insert(
		&self,
		version: Version,
		def: Option<T>,
	) -> Option<Option<T>> {
		let inner = self.inner.write().unwrap();
		if let Some(entry) = inner.versions.get(&version) {
			let old_value = entry.value().clone();
			inner.versions.insert(version, def);
			Some(old_value)
		} else {
			inner.versions.insert(version, def);
			None
		}
	}

	/// Gets the definition that was active at a specific version.
	///
	/// This returns the definition with the highest version that is <= the requested version.
	pub fn get(&self, version: Version) -> Option<T> {
		let inner = self.inner.read().unwrap();

		// Find the entry with the highest version <= requested version
		inner.versions
			.range(..=version)
			.next_back()
			.and_then(|entry| entry.value().clone())
	}

	/// Gets the exact definition at a specific version.
	///
	/// Unlike `get`, this only returns a definition if there's an exact version match.
	pub fn get_exact(&self, version: Version) -> Option<T> {
		let inner = self.inner.read().unwrap();
		inner.versions
			.get(&version)
			.and_then(|entry| entry.value().clone())
	}

	/// Gets the latest (most recent) definition.
	pub fn get_latest(&self) -> Option<T> {
		let inner = self.inner.read().unwrap();
		inner.versions.back().and_then(|entry| entry.value().clone())
	}

	/// Gets all definitions within a version range (inclusive).
	pub fn get_range(
		&self,
		start: Version,
		end: Version,
	) -> Vec<(Version, Option<T>)> {
		let inner = self.inner.read().unwrap();
		inner.versions
			.range(start..=end)
			.map(|entry| (*entry.key(), entry.value().clone()))
			.collect()
	}

	/// Gets all versions that have definitions.
	pub fn versions(&self) -> Vec<Version> {
		let inner = self.inner.read().unwrap();
		inner.versions.iter().map(|entry| *entry.key()).collect()
	}

	/// Removes a definition at a specific version.
	///
	/// Returns the removed definition if one existed.
	pub fn remove(&self, version: Version) -> Option<Option<T>> {
		let inner = self.inner.write().unwrap();
		inner.versions
			.remove(&version)
			.map(|entry| entry.value().clone())
	}

	/// Checks if a definition exists at a specific version.
	pub fn contains_version(&self, version: Version) -> bool {
		let inner = self.inner.read().unwrap();
		inner.versions.contains_key(&version)
	}

	/// Returns the number of versions stored.
	pub fn len(&self) -> usize {
		let inner = self.inner.read().unwrap();
		inner.versions.len()
	}

	/// Checks if the container is empty.
	pub fn is_empty(&self) -> bool {
		let inner = self.inner.read().unwrap();
		inner.versions.is_empty()
	}

	/// Clears all versions.
	pub fn clear(&self) {
		let inner = self.inner.write().unwrap();
		inner.versions.clear();
	}

	/// Performs a bulk insert of multiple versions.
	///
	/// This is more efficient than multiple individual inserts as it acquires
	/// the write lock only once.
	pub fn bulk_insert(
		&self,
		versions: impl IntoIterator<Item = (Version, Option<T>)>,
	) {
		let inner = self.inner.write().unwrap();
		for (version, def) in versions {
			inner.versions.insert(version, def);
		}
	}
}

impl<T: Debug + Clone + Send + Sync + 'static> Clone for VersionedContainer<T> {
	fn clone(&self) -> Self {
		Self {
			inner: Arc::clone(&self.inner),
		}
	}
}

impl<T: Debug + Clone + Send + Sync + 'static> Default
	for VersionedContainer<T>
{
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
	struct TestDef {
		name: String,
	}

	#[test]
	fn test_basic_operations() {
		let versioned = VersionedContainer::<TestDef>::new();

		// Test empty state
		assert!(versioned.is_empty());
		assert_eq!(versioned.len(), 0);
		assert!(versioned.get_latest().is_none());

		// Test insert
		let def1 = TestDef {
			name: "v1".to_string(),
		};
		versioned.insert(1, Some(def1.clone()));
		assert!(!versioned.is_empty());
		assert_eq!(versioned.len(), 1);

		// Test get
		assert_eq!(versioned.get(1), Some(def1.clone()));
		assert_eq!(versioned.get(2), Some(def1.clone())); // Should return v1
		assert_eq!(versioned.get_latest(), Some(def1.clone()));

		// Test multiple versions
		let def2 = TestDef {
			name: "v2".to_string(),
		};
		versioned.insert(5, Some(def2.clone()));
		assert_eq!(versioned.len(), 2);
		assert_eq!(versioned.get(1), Some(def1.clone()));
		assert_eq!(versioned.get(3), Some(def1.clone()));
		assert_eq!(versioned.get(5), Some(def2.clone()));
		assert_eq!(versioned.get(10), Some(def2.clone()));
		assert_eq!(versioned.get_latest(), Some(def2.clone()));

		// Test deletion (None value)
		versioned.insert(7, None);
		assert_eq!(versioned.get(7), None);
		assert_eq!(versioned.get(10), None);

		// Test remove
		versioned.remove(7);
		assert_eq!(versioned.get(10), Some(def2.clone()));
	}

	#[test]
	fn test_range_queries() {
		let versioned = VersionedContainer::<TestDef>::new();

		for i in 0..10 {
			let def = TestDef {
				name: format!("v{}", i),
			};
			versioned.insert(i * 2, Some(def));
		}

		let range = versioned.get_range(4, 10);
		assert_eq!(range.len(), 4); // versions 4, 6, 8, 10

		let versions = versioned.versions();
		assert_eq!(versions.len(), 10);
		assert_eq!(versions[0], 0);
		assert_eq!(versions[9], 18);
	}
}
