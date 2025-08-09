// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Version;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Maximum number of versions to track
const MAX_TRACKED_VERSIONS: usize = 10000;

/// In-memory sequence tracker for CDC events with LRU eviction
/// Tracks the next sequence number to use for each version
pub struct SequenceTracker {
    inner: Mutex<SequenceTrackerInner>,
}

struct SequenceTrackerInner {
    sequences: HashMap<Version, u16>,
    access_order: VecDeque<Version>,
    max_size: usize,
}

impl SequenceTrackerInner {
    /// Update LRU tracking for a version
    fn touch_version(&mut self, version: Version, is_new: bool) {
        if is_new {
            // For new versions, add to access order and potentially evict
            self.access_order.push_back(version);
            
            // Evict oldest if we're over capacity
            while self.access_order.len() > self.max_size {
                if let Some(oldest) = self.access_order.pop_front() {
                    self.sequences.remove(&oldest);
                }
            }
        } else {
            // For existing versions, just update position in access order
            if let Some(pos) = self.access_order.iter().position(|&v| v == version) {
                self.access_order.remove(pos);
            }
            self.access_order.push_back(version);
        }
    }
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self::with_max_size(MAX_TRACKED_VERSIONS)
    }

    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            inner: Mutex::new(SequenceTrackerInner {
                sequences: HashMap::new(),
                access_order: VecDeque::new(),
                max_size,
            }),
        }
    }

    /// Get the next sequence number for a version and increment the counter
    pub fn next_sequence(&self, version: Version) -> u16 {
        let mut inner = self.inner.lock().unwrap();

        // Check if this is a new version
        let is_new = !inner.sequences.contains_key(&version);

        // Get or create the sequence counter
        let sequence = inner.sequences.entry(version).or_insert(0);
        let current = *sequence;
        *sequence = sequence.saturating_add(1);

        // Update LRU tracking
        inner.touch_version(version, is_new);

        current
    }

    /// Reset the sequence counter for a version (useful for testing)
    #[cfg(test)]
    pub fn reset(&self, version: Version) {
        let mut inner = self.inner.lock().unwrap();
        inner.sequences.remove(&version);
        inner.access_order.retain(|&v| v != version);
    }
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_tracking() {
        let tracker = SequenceTracker::new();

        // First sequence for version 1 should be 0
        assert_eq!(tracker.next_sequence(1), 0);
        assert_eq!(tracker.next_sequence(1), 1);
        assert_eq!(tracker.next_sequence(1), 2);

        // Different version starts at 0
        assert_eq!(tracker.next_sequence(2), 0);
        assert_eq!(tracker.next_sequence(2), 1);

        // Version 1 continues from where it left off
        assert_eq!(tracker.next_sequence(1), 3);
    }

    #[test]
    fn test_reset() {
        let tracker = SequenceTracker::new();

        assert_eq!(tracker.next_sequence(1), 0);
        assert_eq!(tracker.next_sequence(1), 1);

        tracker.reset(1);
        assert_eq!(tracker.next_sequence(1), 0);
    }

    #[test]
    fn test_lru_eviction() {
        let tracker = SequenceTracker::with_max_size(3);

        // Fill up to capacity
        assert_eq!(tracker.next_sequence(1), 0);
        assert_eq!(tracker.next_sequence(2), 0);
        assert_eq!(tracker.next_sequence(3), 0);

        // Increment counters
        assert_eq!(tracker.next_sequence(1), 1);
        assert_eq!(tracker.next_sequence(2), 1);
        assert_eq!(tracker.next_sequence(3), 1);

        // At this point, LRU order is [1, 2, 3]
        // Add a 4th version, should evict version 1 (least recently used)
        assert_eq!(tracker.next_sequence(4), 0);

        // Check internal state to verify version 1 was evicted
        {
            let inner = tracker.inner.lock().unwrap();
            assert!(!inner.sequences.contains_key(&1), "Version 1 should be evicted");
            assert!(inner.sequences.contains_key(&2), "Version 2 should still exist");
            assert!(inner.sequences.contains_key(&3), "Version 3 should still exist");
            assert!(inner.sequences.contains_key(&4), "Version 4 should exist");
            assert_eq!(*inner.sequences.get(&2).unwrap(), 2);
            assert_eq!(*inner.sequences.get(&3).unwrap(), 2);
            assert_eq!(*inner.sequences.get(&4).unwrap(), 1);
        }

        // When we access version 1 again, it should start from 0
        assert_eq!(tracker.next_sequence(1), 0);

        // After re-adding version 1, check that version 2 was evicted (it was LRU)
        {
            let inner = tracker.inner.lock().unwrap();
            assert!(!inner.sequences.contains_key(&2), "Version 2 should now be evicted");
            assert!(inner.sequences.contains_key(&1), "Version 1 should exist again");
            assert!(inner.sequences.contains_key(&3), "Version 3 should still exist");
            assert!(inner.sequences.contains_key(&4), "Version 4 should still exist");
        }
    }
}
