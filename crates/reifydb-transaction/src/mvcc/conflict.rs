// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use core::ops::{Bound, RangeBounds};
use std::collections::HashSet;
use reifydb_core::{EncodedKey, EncodedKeyRange};

/// High-performance conflict manager using HashSet for O(1) lookups
/// and optimized range handling
#[derive(Debug, Default, Clone)]
pub struct ConflictManager {
    /// Single key reads - deduplicated automatically by HashSet
    read_keys: HashSet<EncodedKey>,
    /// Range reads - kept separate since they can't be hashed efficiently
    read_ranges: Vec<(Bound<EncodedKey>, Bound<EncodedKey>)>,
    /// Full scan flag
    read_all: bool,
    /// Keys that will be written to
    conflict_keys: HashSet<EncodedKey>,
}

impl ConflictManager {
    pub fn new() -> Self {
        Self {
            read_keys: HashSet::new(),
            read_ranges: Vec::new(),
            read_all: false,
            conflict_keys: HashSet::new(),
        }
    }

    pub fn mark_read(&mut self, key: &EncodedKey) {
        self.read_keys.insert(key.clone());
    }

    pub fn mark_conflict(&mut self, key: &EncodedKey) {
        self.conflict_keys.insert(key.clone());
    }

    pub fn mark_range(&mut self, range: EncodedKeyRange) {
        let start = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        if start == Bound::Unbounded && end == Bound::Unbounded {
            self.read_all = true;
        } else {
            self.read_ranges.push((start, end));
        }
    }

    pub fn mark_iter(&mut self) {
        self.mark_range(EncodedKeyRange::all());
    }

    pub fn has_conflict(&self, other: &Self) -> bool {
        // Fast path: dirty write detection (write-write conflict)
        // Use HashSet intersection for O(min(m,n)) performance
        if !self.conflict_keys.is_disjoint(&other.conflict_keys) {
            return true;
        }

        // Fast path: if no reads, no read-write conflicts possible
        if self.read_keys.is_empty() && self.read_ranges.is_empty() && !self.read_all {
            return false;
        }

        // Check single key read-write conflicts - O(min(reads, writes))
        if !self.read_keys.is_disjoint(&other.conflict_keys) {
            return true;
        }

        // Check range read-write conflicts
        for (start, end) in &self.read_ranges {
            if self.has_range_conflict(start, end, &other.conflict_keys) {
                return true;
            }
        }

        // Check full scan conflicts
        if self.read_all && !other.conflict_keys.is_empty() {
            return true;
        }

        false
    }

    pub fn rollback(&mut self) {
        self.read_keys.clear();
        self.read_ranges.clear();
        self.read_all = false;
        self.conflict_keys.clear();
    }
    
    /// Get all keys that were read by this transaction for efficient conflict detection
    pub fn get_read_keys(&self) -> Vec<EncodedKey> {
        // Only return specific keys, not ranges (handled in oracle)
        self.read_keys.iter().cloned().collect()
    }
    
    /// Get all keys that were written by this transaction
    pub fn get_conflict_keys(&self) -> Vec<EncodedKey> {
        self.conflict_keys.iter().cloned().collect()
    }

    /// Optimized range conflict detection
    #[inline]
    fn has_range_conflict(
        &self,
        start: &Bound<EncodedKey>,
        end: &Bound<EncodedKey>,
        conflict_keys: &HashSet<EncodedKey>,
    ) -> bool {
        // For small conflict sets, linear scan is faster than sorting
        if conflict_keys.len() < 32 {
            return conflict_keys.iter().any(|key| self.key_in_range(key, start, end));
        }

        // For larger sets, convert to sorted vector and use binary search approach
        let mut sorted_keys: Vec<_> = conflict_keys.iter().collect();
        sorted_keys.sort();
        
        self.range_intersects_sorted_keys(&sorted_keys, start, end)
    }

    #[inline]
    fn key_in_range(&self, key: &EncodedKey, start: &Bound<EncodedKey>, end: &Bound<EncodedKey>) -> bool {
        let start_ok = match start {
            Bound::Included(s) => key >= s,
            Bound::Excluded(s) => key > s,
            Bound::Unbounded => true,
        };
        
        let end_ok = match end {
            Bound::Included(e) => key <= e,
            Bound::Excluded(e) => key < e,
            Bound::Unbounded => true,
        };
        
        start_ok && end_ok
    }

    fn range_intersects_sorted_keys(
        &self,
        sorted_keys: &[&EncodedKey],
        start: &Bound<EncodedKey>,
        end: &Bound<EncodedKey>,
    ) -> bool {
        if sorted_keys.is_empty() {
            return false;
        }

        // Find the first key that could be in range
        let start_pos = match start {
            Bound::Included(s) => sorted_keys.binary_search(&s).unwrap_or_else(|pos| pos),
            Bound::Excluded(s) => {
                match sorted_keys.binary_search(&s) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                }
            }
            Bound::Unbounded => 0,
        };

        if start_pos >= sorted_keys.len() {
            return false;
        }

        // Check if the first potentially matching key is actually in range
        let first_key = sorted_keys[start_pos];
        match end {
            Bound::Included(e) => first_key <= e,
            Bound::Excluded(e) => first_key < e,
            Bound::Unbounded => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_key(s: &str) -> EncodedKey {
        EncodedKey::new(s.as_bytes().to_vec())
    }

    #[test]
    fn test_basic_conflict_detection() {
        let mut cm1 = ConflictManager::new();
        let mut cm2 = ConflictManager::new();

        let key = create_key("test");
        cm1.mark_read(&key);
        cm2.mark_conflict(&key);

        assert!(cm1.has_conflict(&cm2));
        assert!(!cm2.has_conflict(&cm1)); // Asymmetric
    }

    #[test]
    fn test_write_write_conflict() {
        let mut cm1 = ConflictManager::new();
        let mut cm2 = ConflictManager::new();

        let key = create_key("test");
        cm1.mark_conflict(&key);
        cm2.mark_conflict(&key);

        assert!(cm1.has_conflict(&cm2));
        assert!(cm2.has_conflict(&cm1)); // Symmetric for write-write
    }

    #[test]
    fn test_no_conflict_different_keys() {
        let mut cm1 = ConflictManager::new();
        let mut cm2 = ConflictManager::new();

        cm1.mark_read(&create_key("key1"));
        cm1.mark_conflict(&create_key("key1"));
        cm2.mark_read(&create_key("key2"));
        cm2.mark_conflict(&create_key("key2"));

        assert!(!cm1.has_conflict(&cm2));
        assert!(!cm2.has_conflict(&cm1));
    }

    #[test]
    fn test_range_conflict() {
        let mut cm1 = ConflictManager::new();
        let mut cm2 = ConflictManager::new();

        // cm1 reads range, cm2 writes within range
        let range = EncodedKeyRange::parse("a..z");
        cm1.mark_range(range);
        
        cm2.mark_conflict(&create_key("m")); // "m" is in range "a..z"

        assert!(cm1.has_conflict(&cm2));
    }

    #[test]
    fn test_deduplication() {
        let mut cm = ConflictManager::new();
        let key = create_key("test");
        
        // Add same key multiple times
        cm.mark_read(&key);
        cm.mark_read(&key);
        cm.mark_read(&key);
        
        // Should only contain one copy
        assert_eq!(cm.get_read_keys().len(), 1);
    }

    #[test]
    fn test_performance_with_many_keys() {
        let mut cm1 = ConflictManager::new();
        let mut cm2 = ConflictManager::new();

        // Add many keys to test HashSet performance
        for i in 0..1000 {
            cm1.mark_read(&create_key(&format!("read_{}", i)));
            cm2.mark_conflict(&create_key(&format!("write_{}", i)));
        }

        // Add one overlapping key
        let shared_key = create_key("shared");
        cm1.mark_read(&shared_key);
        cm2.mark_conflict(&shared_key);

        assert!(cm1.has_conflict(&cm2));
    }

    #[test]
    fn test_iter_functionality() {
        let mut cm1 = ConflictManager::new();
        let mut cm2 = ConflictManager::new();

        cm1.mark_iter(); // Full scan
        cm2.mark_conflict(&create_key("any_key"));

        assert!(cm1.has_conflict(&cm2));
    }
}