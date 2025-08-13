// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::mvcc::types::Pending;
use reifydb_core::{EncodedKey, row::EncodedRow};
use std::collections::{
    BTreeMap,
    btree_map::{IntoIter as BTreeMapIntoIter, Iter as BTreeMapIter, Range as BTreeMapRange},
};
use std::mem::size_of;
use std::ops::RangeBounds;

#[derive(Debug, Default, Clone)]
pub struct PendingWrites {
    /// Primary storage - BTreeMap for sorted key access and range queries
    writes: BTreeMap<EncodedKey, Pending>,
    /// Cached size estimation for batch size limits
    estimated_size: u64,
}

impl PendingWrites {
    /// Create a new empty pending writes manager
    pub fn new() -> Self {
        Self { writes: BTreeMap::new(), estimated_size: 0 }
    }

    /// Returns true if there are no pending writes
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    /// Returns the number of pending writes
    #[inline]
    pub fn len(&self) -> usize {
        self.writes.len()
    }

    /// Returns the maximum batch size in bytes - set high for performance
    #[inline]
    pub fn max_batch_size(&self) -> u64 {
        1024 * 1024 * 1024 // 1GB limit
    }

    /// Returns the maximum number of entries in a batch
    #[inline]
    pub fn max_batch_entries(&self) -> u64 {
        1_000_000 // 1M entries limit
    }

    /// Fast size estimation - uses cached value
    #[inline]
    pub fn estimate_size(&self, _entry: &Pending) -> u64 {
        // Use fixed size estimation for speed
        (size_of::<EncodedKey>() + size_of::<EncodedRow>()) as u64
    }

    /// Get a pending write by key - O(log n) performance
    #[inline]
    pub fn get(&self, key: &EncodedKey) -> Option<&Pending> {
        self.writes.get(key)
    }

    /// Get key-value pair by key - O(log n) performance  
    #[inline]
    pub fn get_entry(&self, key: &EncodedKey) -> Option<(&EncodedKey, &Pending)> {
        self.writes.get_key_value(key)
    }

    /// Check if key exists - O(log n) performance
    #[inline]
    pub fn contains_key(&self, key: &EncodedKey) -> bool {
        self.writes.contains_key(key)
    }

    /// Insert a new pending write - O(log n) performance
    pub fn insert(&mut self, key: EncodedKey, value: Pending) {
        let size_estimate = self.estimate_size(&value);

        if let Some(old_value) = self.writes.insert(key, value) {
            // Update existing - might change size
            let old_size = self.estimate_size(&old_value);
            if size_estimate != old_size {
                self.estimated_size =
                    self.estimated_size.saturating_sub(old_size).saturating_add(size_estimate);
            }
        } else {
            // New entry
            self.estimated_size = self.estimated_size.saturating_add(size_estimate);
        }
    }

    /// Remove an entry by key - O(log n) performance
    pub fn remove_entry(&mut self, key: &EncodedKey) -> Option<(EncodedKey, Pending)> {
        if let Some((removed_key, removed_value)) = self.writes.remove_entry(key) {
            let size_estimate = self.estimate_size(&removed_value);
            self.estimated_size = self.estimated_size.saturating_sub(size_estimate);
            Some((removed_key, removed_value))
        } else {
            None
        }
    }

    /// Iterate over all pending writes - returns BTreeMap iterator for compatibility
    pub fn iter(&self) -> BTreeMapIter<'_, EncodedKey, Pending> {
        self.writes.iter()
    }

    /// Consume and iterate over all pending writes
    pub fn into_iter(self) -> BTreeMapIntoIter<EncodedKey, Pending> {
        self.writes.into_iter()
    }

    /// Clear all pending writes
    pub fn rollback(&mut self) {
        self.writes.clear();
        self.estimated_size = 0;
    }

    /// Get estimated total size of all pending writes
    #[inline]
    pub fn total_estimated_size(&self) -> u64 {
        self.estimated_size
    }

    /// Range query support - returns BTreeMap range iterator for compatibility
    pub fn range<R>(&self, range: R) -> BTreeMapRange<'_, EncodedKey, Pending>
    where
        R: RangeBounds<EncodedKey>,
    {
        self.writes.range(range)
    }

    /// Range query with comparable bounds (same as range for compatibility)
    pub fn range_comparable<R>(&self, range: R) -> BTreeMapRange<'_, EncodedKey, Pending>
    where
        R: RangeBounds<EncodedKey>,
    {
        self.writes.range(range)
    }

    /// Optimized get methods for compatibility (same as regular methods)
    #[inline]
    pub fn get_comparable(&self, key: &EncodedKey) -> Option<&Pending> {
        self.get(key)
    }

    #[inline]
    pub fn get_entry_comparable(&self, key: &EncodedKey) -> Option<(&EncodedKey, &Pending)> {
        self.get_entry(key)
    }

    #[inline]
    pub fn contains_key_comparable(&self, key: &EncodedKey) -> bool {
        self.contains_key(key)
    }

    #[inline]
    pub fn remove_entry_comparable(&mut self, key: &EncodedKey) -> Option<(EncodedKey, Pending)> {
        self.remove_entry(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::types::Pending;
    use reifydb_core::{EncodedKey, Version, row::EncodedRow};

    fn create_test_key(s: &str) -> EncodedKey {
        EncodedKey::new(s.as_bytes())
    }

    fn create_test_row(s: &str) -> EncodedRow {
        EncodedRow(reifydb_core::util::CowVec::new(s.as_bytes().to_vec()))
    }

    fn create_test_pending(version: Version, key: &str, row_data: &str) -> Pending {
        use reifydb_core::delta::Delta;
        Pending {
            delta: Delta::Set { key: create_test_key(key), row: create_test_row(row_data) },
            version,
        }
    }

    #[test]
    fn test_basic_operations() {
        let mut pw = PendingWrites::new();

        assert!(pw.is_empty());
        assert_eq!(pw.len(), 0);

        let key1 = create_test_key("key1");
        let pending1 = create_test_pending(1, "key1", "value1");

        pw.insert(key1.clone(), pending1.clone());

        assert!(!pw.is_empty());
        assert_eq!(pw.len(), 1);
        assert!(pw.contains_key(&key1));
        assert_eq!(pw.get(&key1).unwrap(), &pending1);
    }

    #[test]
    fn test_update_operations() {
        let mut pw = PendingWrites::new();
        let key = create_test_key("key");

        let pending1 = create_test_pending(1, "key", "value1");
        let pending2 = create_test_pending(2, "key", "value2");

        pw.insert(key.clone(), pending1);
        assert_eq!(pw.len(), 1);

        pw.insert(key.clone(), pending2.clone());
        assert_eq!(pw.len(), 1); // Still 1, just updated
        assert_eq!(pw.get(&key).unwrap(), &pending2);
    }

    #[test]
    fn test_range_operations() {
        let mut pw = PendingWrites::new();

        for i in 0..10 {
            let key = create_test_key(&format!("key{:02}", i));
            let pending =
                create_test_pending(i as Version, &format!("key{:02}", i), &format!("value{}", i));
            pw.insert(key, pending);
        }

        let start = create_test_key("key03");
        let end = create_test_key("key07");

        let range_results: Vec<_> = pw.range(start..end).collect();
        assert_eq!(range_results.len(), 4); // key03, key04, key05, key06
    }

    #[test]
    fn test_iterator_compatibility() {
        let mut pw = PendingWrites::new();

        // Test that iterators work with transaction system expectations
        for i in 0..5 {
            let key = create_test_key(&format!("key{}", i));
            let pending =
                create_test_pending(i as Version, &format!("key{}", i), &format!("value{}", i));
            pw.insert(key, pending);
        }

        // Test that iter() returns the expected BTreeMap iterator type
        let iter = pw.iter();
        let items: Vec<_> = iter.collect();
        assert_eq!(items.len(), 5);

        // Test that the iterator is ordered (important for BTreeMap)
        let keys: Vec<_> = items.iter().map(|(k, _)| k).collect();
        let mut expected_keys = keys.clone();
        expected_keys.sort();
        assert_eq!(keys, expected_keys);

        // Test range queries
        let start = create_test_key("key1");
        let end = create_test_key("key4");
        let range_items: Vec<_> = pw.range(start..end).collect();
        assert_eq!(range_items.len(), 3); // key1, key2, key3
    }

    #[test]
    fn test_performance_operations() {
        let mut pw = PendingWrites::new();

        // Test with larger dataset to verify performance characteristics
        for i in 0..1000 {
            let key = create_test_key(&format!("key{:06}", i));
            let pending =
                create_test_pending(i as Version, &format!("key{:06}", i), &format!("value{}", i));
            pw.insert(key, pending);
        }

        assert_eq!(pw.len(), 1000);

        // Test fast lookups
        let lookup_key = create_test_key("key000500");
        assert!(pw.contains_key(&lookup_key));
        assert!(pw.get(&lookup_key).is_some());

        // Test removal
        let removed = pw.remove_entry(&lookup_key);
        assert!(removed.is_some());
        assert_eq!(pw.len(), 999);
        assert!(!pw.contains_key(&lookup_key));
    }

    #[test]
    fn test_rollback() {
        let mut pw = PendingWrites::new();

        for i in 0..10 {
            let key = create_test_key(&format!("key{}", i));
            let pending =
                create_test_pending(i as Version, &format!("key{}", i), &format!("value{}", i));
            pw.insert(key, pending);
        }

        assert_eq!(pw.len(), 10);
        assert!(pw.total_estimated_size() > 0);

        pw.rollback();

        assert!(pw.is_empty());
        assert_eq!(pw.total_estimated_size(), 0);
    }
}
