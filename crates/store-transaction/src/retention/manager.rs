use std::collections::HashMap;

use reifydb_core::{
    retention::{CleanupAction, CleanupMode, RetentionPolicy},
    interface::{SourceId, FlowNodeId},
    key::{SourceRetentionPolicyKey, OperatorRetentionPolicyKey, RowKey, FlowNodeStateKey},
    CommitVersion, EncodedKey,
};

use crate::Result;
use super::{cleaner::RetentionCleaner, two_stage::TwoStageCleanupTracker};

/// Manages retention policies for sources and operators
pub struct RetentionPolicyManager {
    cleaner: RetentionCleaner,
    two_stage_tracker: TwoStageCleanupTracker,
    /// Cache of source retention policies
    source_policies: HashMap<SourceId, RetentionPolicy>,
    /// Cache of operator retention policies
    operator_policies: HashMap<FlowNodeId, RetentionPolicy>,
}

impl RetentionPolicyManager {
    pub fn new() -> Self {
        Self {
            cleaner: RetentionCleaner::new(),
            two_stage_tracker: TwoStageCleanupTracker::new(),
            source_policies: HashMap::new(),
            operator_policies: HashMap::new(),
        }
    }

    /// Apply retention policy for a source (table/view/ringbuffer)
    /// TODO: This needs to be integrated with the actual store implementation
    pub  fn apply_source_retention(
        &mut self,
        _source_id: SourceId,
        _current_version: CommitVersion,
    ) -> Result<()> {
        // Placeholder for now
        Ok(())
    }

    /// Apply retention policy for a flow operator
    pub  fn apply_operator_retention<T: TransactionStore + MultiVersionGet>(
        &mut self,
        store: &T,
        node_id: FlowNodeId,
        current_version: CommitVersion,
    ) -> Result<()> {
        // Get the retention policy for this operator
        let policy = match self.get_operator_policy(store, node_id)? {
            Some(p) => p,
            None => return Ok(()), // No policy, keep everything
        };

        if matches!(policy, RetentionPolicy::KeepForever) {
            return Ok(()); // Nothing to clean up
        }

        // Find all keys for this operator
        let keys = self.find_keys_for_operator(store, node_id)?;

        // Process each key based on the policy
        for key in keys {
            let action = self.determine_action(&key, &policy, current_version)?;

            match action {
                CleanupAction::Keep => continue,
                _ => {
                    // Process the cleanup action
                    self.cleaner.process_cleanup(store, vec![key], current_version, action)?;
                }
            }
        }

        Ok(())
    }

    /// Get retention policy for a source
    pub  fn get_source_policy<T: TransactionStore + MultiVersionGet>(
        &mut self,
        store: &T,
        source_id: SourceId,
    ) -> Result<Option<RetentionPolicy>> {
        // Check cache first
        if let Some(policy) = self.source_policies.get(&source_id) {
            return Ok(Some(policy.clone()));
        }

        // Query from store
        let key = SourceRetentionPolicyKey::encoded(source_id);

        if let Some(values) = store.get(&key, CommitVersion(u64::MAX))? {
            // Decode the policy from values
            // Note: This will require access to the catalog's decode function
            // For now, we'll keep this as a TODO and mark it for future implementation
            // TODO: Import and use catalog's decode_retention_policy function
        }

        Ok(None)
    }

    /// Get retention policy for an operator
    pub  fn get_operator_policy<T: TransactionStore + MultiVersionGet>(
        &mut self,
        store: &T,
        node_id: FlowNodeId,
    ) -> Result<Option<RetentionPolicy>> {
        // Check cache first
        if let Some(policy) = self.operator_policies.get(&node_id) {
            return Ok(Some(policy.clone()));
        }

        // Query from store
        let key = OperatorRetentionPolicyKey::encoded(node_id);

        if let Some(values) = store.get(&key, CommitVersion(u64::MAX))? {
            // Decode the policy from values
            // TODO: Import and use catalog's decode_retention_policy function
        }

        Ok(None)
    }

    /// Set retention policy for a source
    pub  fn set_source_policy<T: TransactionStore>(
        &mut self,
        store: &T,
        source_id: SourceId,
        policy: RetentionPolicy,
        version: CommitVersion,
    ) -> Result<()> {
        let key = SourceRetentionPolicyKey::encoded(source_id);
        let value = serde_json::to_vec(&policy)?;

        // Store in the transaction store
        // This will need to be integrated with the actual store API
        // For now, we cache it
        self.source_policies.insert(source_id, policy);

        Ok(())
    }

    /// Set retention policy for an operator
    pub  fn set_operator_policy<T: TransactionStore>(
        &mut self,
        store: &T,
        node_id: FlowNodeId,
        policy: RetentionPolicy,
        version: CommitVersion,
    ) -> Result<()> {
        let key = OperatorRetentionPolicyKey::encoded(node_id);
        let value = serde_json::to_vec(&policy)?;

        // Store in the transaction store
        // This will need to be integrated with the actual store API
        // For now, we cache it
        self.operator_policies.insert(node_id, policy);

        Ok(())
    }

    /// Find all keys belonging to a source
     fn find_keys_for_source<T: TransactionStore + MultiVersionGet>(
        &self,
        store: &T,
        source_id: SourceId,
    ) -> Result<Vec<EncodedKey>> {
        // Use RowKey range scanning to find all rows for this source
        let range = RowKey::source_range(source_id);

        // This will need to be implemented based on the store's range scan capabilities
        // For now, return empty
        Ok(Vec::new())
    }

    /// Find all keys belonging to an operator
     fn find_keys_for_operator<T: TransactionStore + MultiVersionGet>(
        &self,
        store: &T,
        node_id: FlowNodeId,
    ) -> Result<Vec<EncodedKey>> {
        // Use FlowNodeStateKey range scanning to find all state for this operator
        let range = FlowNodeStateKey::node_range(node_id);

        // This will need to be implemented based on the store's range scan capabilities
        // For now, return empty
        Ok(Vec::new())
    }

    /// Determine the cleanup action for a key based on policy
    fn determine_action(
        &mut self,
        _key: &EncodedKey,
        policy: &RetentionPolicy,
        _current_version: CommitVersion,
    ) -> Result<CleanupAction> {
        match policy {
            RetentionPolicy::KeepForever => Ok(CleanupAction::Keep),
            RetentionPolicy::KeepVersions { cleanup_mode, .. } => {
                // For this policy, check if cleanup is needed
                // This would require checking version counts
                // For now, we'll use the cleanup mode directly
                match cleanup_mode {
                    CleanupMode::Delete => Ok(CleanupAction::Delete),
                    CleanupMode::Drop => Ok(CleanupAction::Drop),
                }
            }
        }
    }

    /// Clear all cached policies
    pub fn clear_cache(&mut self) {
        self.source_policies.clear();
        self.operator_policies.clear();
    }

    /// Get statistics about retention management
    pub fn get_stats(&self) -> RetentionStats {
        RetentionStats {
            cached_source_policies: self.source_policies.len(),
            cached_operator_policies: self.operator_policies.len(),
            tracked_deletions: self.two_stage_tracker.tracked_count(),
        }
    }
}

/// Statistics about retention management
#[derive(Debug, Clone)]
pub struct RetentionStats {
    pub cached_source_policies: usize,
    pub cached_operator_policies: usize,
    pub tracked_deletions: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_manager_creation() {
        let manager = RetentionPolicyManager::new();
        let stats = manager.get_stats();

        assert_eq!(stats.cached_source_policies, 0);
        assert_eq!(stats.cached_operator_policies, 0);
        assert_eq!(stats.tracked_deletions, 0);
    }
}