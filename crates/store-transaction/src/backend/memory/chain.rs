// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, value::encoded::EncodedValues};

/// A compact version chain that stores versions in descending order (newest first)
/// for efficient lookup of the latest version <= requested version
#[derive(Default, Clone, Debug)]
pub struct VersionChain {
    // Stored newest first: (version, values)
    // None represents a tombstone (deletion)
    entries: Vec<(CommitVersion, Option<EncodedValues>)>,
}

impl VersionChain {
    /// Create a new empty version chain
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Insert or update a version in the chain, maintaining descending order
    #[inline]
    pub fn set(&mut self, version: CommitVersion, values: Option<EncodedValues>) {
        // Binary search with reverse comparison for descending order
        match self.entries.binary_search_by(|(v, _)| v.cmp(&version).reverse()) {
            Ok(i) => {
                // Version already exists, update it
                self.entries[i] = (version, values);
            }
            Err(i) => {
                // Insert at the correct position to maintain descending order
                self.entries.insert(i, (version, values));
            }
        }
    }

    /// Get the value at a specific version (latest version <= requested)
    #[inline]
    pub fn get_at(&self, version: CommitVersion) -> Option<Option<EncodedValues>> {
        // First version <= requested version
        for (v, value) in &self.entries {
            if *v <= version {
                return Some(value.clone());
            }
        }
        None
    }

    /// Check if a key exists at a specific version (not a tombstone)
    #[inline]
    pub fn contains_at(&self, version: CommitVersion) -> bool {
        self.get_at(version).map_or(false, |v| v.is_some())
    }

    /// Get the latest version and value in the chain
    #[inline]
    pub fn get_latest(&self) -> Option<(CommitVersion, Option<EncodedValues>)> {
        self.entries.first().cloned()
    }

    /// Get the latest non-tombstone value
    #[inline]
    pub fn get_latest_value(&self) -> Option<EncodedValues> {
        self.entries
            .iter()
            .find_map(|(_, v)| v.as_ref().cloned())
    }

    /// Get the latest version number
    #[inline]
    pub fn get_latest_version(&self) -> Option<CommitVersion> {
        self.entries.first().map(|(v, _)| *v)
    }

    /// Remove versions older than the given version (for future GC)
    pub fn compact(&mut self, oldest_required: CommitVersion) {
        // Keep all versions >= oldest_required
        // Since entries are in descending order, we can truncate from the end
        if let Some(pos) = self.entries.iter().position(|(v, _)| *v < oldest_required) {
            self.entries.truncate(pos);
            // Free unused capacity to actually release memory back to allocator
            self.entries.shrink_to_fit();
        }
    }

    /// Check if the chain is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of versions in the chain
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::CowVec;

    fn test_values() -> EncodedValues {
        EncodedValues(CowVec::new(vec![]))
    }

    #[test]
    fn test_version_chain_ordering() {
        let mut chain = VersionChain::new();

        // Insert versions out of order
        chain.set(5.into(), Some(test_values()));
        chain.set(2.into(), Some(test_values()));
        chain.set(7.into(), Some(test_values()));
        chain.set(3.into(), Some(test_values()));

        // Verify descending order
        assert_eq!(chain.entries[0].0, CommitVersion::from(7));
        assert_eq!(chain.entries[1].0, CommitVersion::from(5));
        assert_eq!(chain.entries[2].0, CommitVersion::from(3));
        assert_eq!(chain.entries[3].0, CommitVersion::from(2));
    }

    #[test]
    fn test_get_at_version() {
        let mut chain = VersionChain::new();

        chain.set(2.into(), Some(test_values()));
        chain.set(5.into(), Some(test_values()));
        chain.set(8.into(), Some(test_values()));

        // Query at exact versions
        assert!(chain.get_at(2.into()).unwrap().is_some());
        assert!(chain.get_at(5.into()).unwrap().is_some());
        assert!(chain.get_at(8.into()).unwrap().is_some());

        // Query between versions (should get the latest <= requested)
        assert!(chain.get_at(3.into()).unwrap().is_some()); // Gets version 2
        assert!(chain.get_at(6.into()).unwrap().is_some()); // Gets version 5
        assert!(chain.get_at(10.into()).unwrap().is_some()); // Gets version 8

        // Query before first version
        assert!(chain.get_at(1.into()).is_none());
    }

    #[test]
    fn test_tombstones() {
        let mut chain = VersionChain::new();

        // Set a value
        chain.set(1.into(), Some(test_values()));
        assert!(chain.contains_at(1.into()));

        // Set a tombstone
        chain.set(2.into(), None);
        assert!(!chain.contains_at(2.into())); // Tombstone means not contained

        // Value still exists at version 1
        assert!(chain.contains_at(1.into()));

        // Set another value after tombstone
        chain.set(3.into(), Some(test_values()));
        assert!(chain.contains_at(3.into()));
        assert!(!chain.contains_at(2.into())); // Still tombstoned at v2
    }

    #[test]
    fn test_update_existing_version() {
        let mut chain = VersionChain::new();

        chain.set(5.into(), Some(test_values()));
        assert_eq!(chain.len(), 1);

        // Update the same version
        chain.set(5.into(), None); // Change to tombstone
        assert_eq!(chain.len(), 1); // Still only one entry
        assert!(!chain.contains_at(5.into())); // Now a tombstone
    }

    #[test]
    fn test_compact() {
        let mut chain = VersionChain::new();

        for v in 1..=10 {
            chain.set(v.into(), Some(test_values()));
        }
        assert_eq!(chain.len(), 10);

        // Compact to keep only versions >= 5
        chain.compact(5.into());
        assert_eq!(chain.len(), 6); // Versions 5-10

        // Verify remaining versions
        assert!(chain.get_at(5.into()).is_some());
        assert!(chain.get_at(4.into()).is_none()); // Compacted away
    }
}