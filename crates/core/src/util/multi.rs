// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::Debug,
	sync::{Arc, RwLock},
};

use crossbeam_skiplist::SkipMap;

use crate::common::CommitVersion;

#[derive(Debug)]
pub struct MultiVersionContainer<T: Debug + Clone + Send + Sync + 'static> {
	inner: Arc<RwLock<MultiVersionDefInner<T>>>,
}

#[derive(Debug)]
struct MultiVersionDefInner<T: Debug + Clone + Send + Sync + 'static> {
	versions: SkipMap<CommitVersion, Option<T>>,
}

impl<T: Debug + Clone + Send + Sync + 'static> MultiVersionContainer<T> {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(MultiVersionDefInner {
				versions: SkipMap::new(),
			})),
		}
	}

	pub fn insert(&self, version: impl Into<CommitVersion>, value: T) -> Option<Option<T>> {
		let version = version.into();
		let inner = self.inner.write().unwrap();
		if let Some(entry) = inner.versions.get(&version) {
			let old_value = entry.value().clone();
			inner.versions.insert(version, Some(value));
			Some(old_value)
		} else {
			inner.versions.insert(version, Some(value));
			None
		}
	}

	pub fn get(&self, version: impl Into<CommitVersion>) -> Option<T> {
		let version = version.into();
		let inner = self.inner.read().unwrap();

		inner.versions.range(..=version).next_back().and_then(|entry| entry.value().clone())
	}

	pub fn get_or_tombstone(&self, version: impl Into<CommitVersion>) -> Option<Option<T>> {
		let version = version.into();
		let inner = self.inner.read().unwrap();

		inner.versions.range(..=version).next_back().map(|entry| entry.value().clone())
	}

	pub fn get_latest(&self) -> Option<T> {
		let inner = self.inner.read().unwrap();
		inner.versions.back().and_then(|entry| entry.value().clone())
	}

	pub fn versions(&self) -> Vec<CommitVersion> {
		let inner = self.inner.read().unwrap();
		inner.versions.iter().map(|entry| *entry.key()).collect()
	}

	pub fn remove(&self, version: impl Into<CommitVersion>) -> Option<Option<T>> {
		let version = version.into();
		let inner = self.inner.write().unwrap();

		if let Some(entry) = inner.versions.get(&version) {
			let old_value = entry.value().clone();
			inner.versions.insert(version, None);
			Some(old_value)
		} else {
			inner.versions.insert(version, None);
			None
		}
	}

	pub fn len(&self) -> usize {
		let inner = self.inner.read().unwrap();
		inner.versions.len()
	}

	pub fn is_empty(&self) -> bool {
		let inner = self.inner.read().unwrap();
		inner.versions.is_empty()
	}

	pub fn clear(&self) {
		let inner = self.inner.write().unwrap();
		inner.versions.clear();
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
pub mod tests {
	use super::*;

	#[derive(Debug, Clone, PartialEq)]
	struct Test {
		name: String,
	}

	#[test]
	fn test_basic_operations() {
		let multi = MultiVersionContainer::<Test>::new();

		// Test empty state
		assert!(multi.is_empty());
		assert_eq!(multi.len(), 0);
		assert!(multi.get_latest().is_none());

		// Test insert
		let def1 = Test {
			name: "v1".to_string(),
		};
		multi.insert(1, def1.clone());
		assert!(!multi.is_empty());
		assert_eq!(multi.len(), 1);

		// Test get
		assert_eq!(multi.get(1), Some(def1.clone()));
		assert_eq!(multi.get(2), Some(def1.clone())); // Should return v1
		assert_eq!(multi.get_latest(), Some(def1.clone()));

		// Test multiple versions
		let def2 = Test {
			name: "v2".to_string(),
		};
		multi.insert(5, def2.clone());
		assert_eq!(multi.len(), 2);
		assert_eq!(multi.get(1), Some(def1.clone()));
		assert_eq!(multi.get(3), Some(def1.clone()));
		assert_eq!(multi.get(5), Some(def2.clone()));
		assert_eq!(multi.get(10), Some(def2.clone()));
		assert_eq!(multi.get_latest(), Some(def2.clone()));

		multi.remove(7);
		assert_eq!(multi.get(7), None);
		assert_eq!(multi.get(10), None);

		assert_eq!(multi.get_latest(), None);
	}
}
