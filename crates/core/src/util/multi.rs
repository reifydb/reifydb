// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	fmt::Debug,
	sync::{Arc, RwLock},
};

use crossbeam_skiplist::SkipMap;

use crate::CommitVersion;

/// A thread-safe container for multi values.
///
/// This structure maintains multiple versions of a definition, allowing
/// for point-in-time queries and concurrent access patterns.
#[derive(Debug)]
pub struct MultiVersionContainer<T: Debug + Clone + Send + Sync + 'static> {
	inner: Arc<RwLock<MultiVersionDefInner<T>>>,
}

#[derive(Debug)]
struct MultiVersionDefInner<T: Debug + Clone + Send + Sync + 'static> {
	versions: SkipMap<CommitVersion, Option<T>>,
}

impl<T: Debug + Clone + Send + Sync + 'static> MultiVersionContainer<T> {
	/// Creates a new empty multi definition container.
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(MultiVersionDefInner {
				versions: SkipMap::new(),
			})),
		}
	}

	/// Creates a new multi definition with initial data.
	pub fn with_initial(version: CommitVersion, def: Option<T>) -> Self {
		let multi = Self::new();
		multi.insert(version, def);
		multi
	}

	/// Inserts a definition at a specific version.
	///
	/// Returns the previous definition at this version if one existed.
	pub fn insert(&self, version: CommitVersion, def: Option<T>) -> Option<Option<T>> {
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
	/// This returns the definition with the highest version that is <= the
	/// requested version.
	pub fn get(&self, version: CommitVersion) -> Option<T> {
		let inner = self.inner.read().unwrap();

		// Find the entry with the highest version <= requested version
		inner.versions.range(..=version).next_back().and_then(|entry| entry.value().clone())
	}

	/// Gets the exact definition at a specific version.
	///
	/// Unlike `get`, this only returns a definition if there's an exact
	/// version match.
	pub fn get_exact(&self, version: CommitVersion) -> Option<T> {
		let inner = self.inner.read().unwrap();
		inner.versions.get(&version).and_then(|entry| entry.value().clone())
	}

	/// Gets the latest (most recent) definition.
	pub fn get_latest(&self) -> Option<T> {
		let inner = self.inner.read().unwrap();
		inner.versions.back().and_then(|entry| entry.value().clone())
	}

	/// Gets all definitions within a version range (inclusive).
	pub fn get_range(&self, start: CommitVersion, end: CommitVersion) -> Vec<(CommitVersion, Option<T>)> {
		let inner = self.inner.read().unwrap();
		inner.versions.range(start..=end).map(|entry| (*entry.key(), entry.value().clone())).collect()
	}

	/// Gets all versions that have definitions.
	pub fn versions(&self) -> Vec<CommitVersion> {
		let inner = self.inner.read().unwrap();
		inner.versions.iter().map(|entry| *entry.key()).collect()
	}

	/// Removes a definition at a specific version.
	///
	/// Returns the removed definition if one existed.
	pub fn remove(&self, version: CommitVersion) -> Option<Option<T>> {
		let inner = self.inner.write().unwrap();
		inner.versions.remove(&version).map(|entry| entry.value().clone())
	}

	/// Checks if a definition exists at a specific version.
	pub fn contains_version(&self, version: CommitVersion) -> bool {
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
	/// This is more efficient than multiple individual inserts as it
	/// acquires the write lock only once.
	pub fn bulk_insert(&self, versions: impl IntoIterator<Item = (CommitVersion, Option<T>)>) {
		let inner = self.inner.write().unwrap();
		for (version, def) in versions {
			inner.versions.insert(version, def);
		}
	}
}

impl<T: Debug + Clone + Send + Sync + 'static> Clone for MultiVersionContainer<T> {
	fn clone(&self) -> Self {
		Self {
			inner: Arc::clone(&self.inner),
		}
	}
}

impl<T: Debug + Clone + Send + Sync + 'static> Default for MultiVersionContainer<T> {
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
		let multi = MultiVersionContainer::<TestDef>::new();

		// Test empty state
		assert!(multi.is_empty());
		assert_eq!(multi.len(), 0);
		assert!(multi.get_latest().is_none());

		// Test insert
		let def1 = TestDef {
			name: "v1".to_string(),
		};
		multi.insert(1, Some(def1.clone()));
		assert!(!multi.is_empty());
		assert_eq!(multi.len(), 1);

		// Test get
		assert_eq!(multi.get(1), Some(def1.clone()));
		assert_eq!(multi.get(2), Some(def1.clone())); // Should return v1
		assert_eq!(multi.get_latest(), Some(def1.clone()));

		// Test multiple versions
		let def2 = TestDef {
			name: "v2".to_string(),
		};
		multi.insert(5, Some(def2.clone()));
		assert_eq!(multi.len(), 2);
		assert_eq!(multi.get(1), Some(def1.clone()));
		assert_eq!(multi.get(3), Some(def1.clone()));
		assert_eq!(multi.get(5), Some(def2.clone()));
		assert_eq!(multi.get(10), Some(def2.clone()));
		assert_eq!(multi.get_latest(), Some(def2.clone()));

		// Test deletion (None value)
		multi.insert(7, None);
		assert_eq!(multi.get(7), None);
		assert_eq!(multi.get(10), None);

		// Test remove
		multi.remove(7);
		assert_eq!(multi.get(10), Some(def2.clone()));
	}

	#[test]
	fn test_range_queries() {
		let multi = MultiVersionContainer::<TestDef>::new();

		for i in 0..10 {
			let def = TestDef {
				name: format!("v{}", i),
			};
			multi.insert(i * 2, Some(def));
		}

		let range = multi.get_range(4, 10);
		assert_eq!(range.len(), 4); // versions 4, 6, 8, 10

		let versions = multi.versions();
		assert_eq!(versions.len(), 10);
		assert_eq!(versions[0], 0);
		assert_eq!(versions[9], 18);
	}
}
