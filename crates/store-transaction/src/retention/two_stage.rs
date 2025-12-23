use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reifydb_core::{
    retention::{CleanupAction, RetentionPolicy},
    CommitVersion, EncodedKey,
};

/// Tracks deletion times for two-stage cleanup (Delete then Drop)
/// Note: Two-stage cleanup has been simplified and removed
#[derive(Debug, Clone)]
pub struct TwoStageCleanupTracker {
    /// Maps keys to their deletion version and timestamp
    deletion_times: HashMap<EncodedKey, DeletionRecord>,
}

#[derive(Debug, Clone)]
struct DeletionRecord {
    /// Version when the key was deleted (tombstoned)
    deletion_version: CommitVersion,
    /// Unix timestamp when deletion occurred
    deletion_timestamp: u64,
}

impl TwoStageCleanupTracker {
    pub fn new() -> Self {
        Self {
            deletion_times: HashMap::new(),
        }
    }

    /// Process a key for two-stage cleanup
    /// Note: Two-stage cleanup is no longer supported after simplification
    pub fn process_cleanup(
        &mut self,
        _key: &EncodedKey,
        _policy: &RetentionPolicy,
        _current_version: CommitVersion,
    ) -> CleanupAction {
        // Two-stage cleanup has been removed
        CleanupAction::Keep
    }

    /// Process Delete-then-Drop logic
    fn process_delete_then_drop(
        &mut self,
        key: &EncodedKey,
        drop_after: &Duration,
        current_version: CommitVersion,
    ) -> CleanupAction {
        if let Some(record) = self.deletion_times.get(key) {
            // Key was previously deleted, check if it's time to drop
            if self.should_drop(record, drop_after) {
                // Remove from tracking after drop
                self.deletion_times.remove(key);
                CleanupAction::Drop
            } else {
                // Still within retention period
                CleanupAction::Keep
            }
        } else {
            // Key is live, mark for deletion
            self.mark_for_deletion(key.clone(), current_version);
            CleanupAction::Delete
        }
    }

    /// Mark a key as deleted (tombstoned)
    pub fn mark_for_deletion(&mut self, key: EncodedKey, version: CommitVersion) {
        let record = DeletionRecord {
            deletion_version: version,
            deletion_timestamp: Self::current_timestamp(),
        };
        self.deletion_times.insert(key, record);
    }

    /// Check if enough time has passed to drop the tombstone
    fn should_drop(&self, record: &DeletionRecord, drop_after: &Duration) -> bool {
        let current_time = Self::current_timestamp();
        let elapsed_seconds = current_time.saturating_sub(record.deletion_timestamp);
        elapsed_seconds >= drop_after.as_secs()
    }

    /// Get the current Unix timestamp
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }


    /// Get the number of keys being tracked
    pub fn tracked_count(&self) -> usize {
        self.deletion_times.len()
    }

    /// Clear all tracking data
    pub fn clear(&mut self) {
        self.deletion_times.clear();
    }

    /// Remove tracking for a specific key
    pub fn remove_tracking(&mut self, key: &EncodedKey) -> bool {
        self.deletion_times.remove(key).is_some()
    }

    /// Check if a key is being tracked
    pub fn is_tracked(&self, key: &EncodedKey) -> bool {
        self.deletion_times.contains_key(key)
    }

    /// Get deletion info for a key
    pub fn get_deletion_info(&self, key: &EncodedKey) -> Option<(CommitVersion, u64)> {
        self.deletion_times.get(key).map(|record| {
            (record.deletion_version, record.deletion_timestamp)
        })
    }

    /// Clean up expired entries that are past their drop time
    pub fn cleanup_expired(&mut self, policies: &HashMap<EncodedKey, Duration>) {
        let current_time = Self::current_timestamp();

        self.deletion_times.retain(|key, record| {
            if let Some(drop_after) = policies.get(key) {
                let elapsed = current_time.saturating_sub(record.deletion_timestamp);
                // Keep if not yet expired
                elapsed < drop_after.as_secs()
            } else {
                // No policy found, keep the record
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn test_two_stage_tracker_basic() {
        let mut tracker = TwoStageCleanupTracker::new();
        let key = EncodedKey::new(vec![1, 2, 3]);
        let version = CommitVersion(100);

        // Initially not tracked
        assert!(!tracker.is_tracked(&key));

        // Mark for deletion
        tracker.mark_for_deletion(key.clone(), version);
        assert!(tracker.is_tracked(&key));
        assert_eq!(tracker.tracked_count(), 1);

        // Get deletion info
        let info = tracker.get_deletion_info(&key);
        assert!(info.is_some());
        let (del_version, _timestamp) = info.unwrap();
        assert_eq!(del_version, version);
    }

    #[test]
    fn test_delete_then_drop_flow() {
        let mut tracker = TwoStageCleanupTracker::new();
        let key = EncodedKey::new(vec![1, 2, 3]);
        let version = CommitVersion(100);
        let policy = RetentionPolicy::KeepForever; // Simplified - two-stage removed

        // process_cleanup now always returns Keep
        let action = tracker.process_cleanup(&key, &policy, version);
        assert_eq!(action, CleanupAction::Keep);
    }


    #[tokio::test]
    async fn test_cleanup_expired() {
        let mut tracker = TwoStageCleanupTracker::new();
        let key1 = EncodedKey::new(vec![1, 2, 3]);
        let key2 = EncodedKey::new(vec![4, 5, 6]);

        tracker.mark_for_deletion(key1.clone(), CommitVersion(100));

        // Wait a bit
        sleep(Duration::from_millis(100)).await;
        tracker.mark_for_deletion(key2.clone(), CommitVersion(200));

        // Create policies
        let mut policies = HashMap::new();
        policies.insert(key1.clone(), Duration::from_millis(50)); // Already expired
        policies.insert(key2.clone(), Duration::from_secs(60)); // Not expired

        // Cleanup expired
        tracker.cleanup_expired(&policies);

        // key1 should be removed, key2 should remain
        assert!(!tracker.is_tracked(&key1));
        assert!(tracker.is_tracked(&key2));
        assert_eq!(tracker.tracked_count(), 1);
    }
}