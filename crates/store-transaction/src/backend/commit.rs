// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::BTreeMap;

use reifydb_core::{CommitVersion, CowVec, delta::Delta};

/// A buffered commit waiting to be applied
#[derive(Debug, Clone)]
pub struct BufferedCommit {
	pub deltas: CowVec<Delta>,
	pub version: CommitVersion,
	pub timestamp: u64,
}

/// Buffer for ordering multi-version commits
///
/// This buffer ensures that commits are applied in strictly monotonically increasing order
/// by version number. Out-of-order commits are buffered until all preceding commits arrive.
#[derive(Debug)]
pub struct CommitBuffer {
	/// Buffered commits waiting to be applied
	pub(crate) buffer: BTreeMap<CommitVersion, BufferedCommit>,
	/// The next version we expect to apply. None means we haven't seen any commits yet.
	pub(crate) next_expected: Option<CommitVersion>,
}

impl CommitBuffer {
	/// Create a new empty commit buffer
	pub fn new() -> Self {
		Self {
			buffer: BTreeMap::new(),
			next_expected: None,
		}
	}

	/// Add a commit to the buffer
	///
	/// Returns true if this commit can be immediately applied (it's the next expected version)
	pub fn add_commit(
		&mut self,
		version: CommitVersion,
		deltas: CowVec<Delta>,
		timestamp: u64,
	) -> bool {
		// If this is the first commit we've seen, set it as our baseline
		if self.next_expected.is_none() {
			self.next_expected = Some(version);
		}

		let commit = BufferedCommit {
			deltas,
			version,
			timestamp,
		};

		self.buffer.insert(version, commit);

		Some(version) == self.next_expected
	}

	/// Drain all commits that are ready to be applied in order
	///
	/// Returns commits in version order, starting from the next expected version.
	/// Updates the next expected version as commits are drained.
	pub fn drain_ready(&mut self) -> Vec<BufferedCommit> {
		// If we haven't established a baseline yet, nothing is ready
		let Some(mut current_version) = self.next_expected else {
			return vec![];
		};

		let mut result = Vec::new();
		while let Some(commit) = self.buffer.remove(&current_version) {
			result.push(commit);
			current_version = CommitVersion(current_version.0 + 1);
		}

		// Update the next expected version
		if !result.is_empty() {
			self.next_expected = Some(current_version);
		}

		result
	}
}

impl Default for CommitBuffer {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_empty_buffer_accepts_first_commit_as_baseline() {
		let mut buffer = CommitBuffer::new();

		// First commit establishes the baseline
		let ready = buffer.add_commit(CommitVersion(42), CowVec::new(vec![]),  0);

		assert!(ready);
		assert_eq!(buffer.next_expected.unwrap(), 42);

		let commits = buffer.drain_ready();
		assert_eq!(commits.len(), 1);
		assert_eq!(commits[0].version, 42);
		assert_eq!(buffer.next_expected.unwrap(), 43);
	}

	#[test]
	fn test_in_order_commits() {
		let mut buffer = CommitBuffer::new();

		// First commit establishes baseline at version 10
		assert!(buffer.add_commit(CommitVersion(10), CowVec::new(vec![]),  0));
		let ready = buffer.drain_ready();
		assert_eq!(ready.len(), 1);
		assert_eq!(ready[0].version, 10);

		// Next commits in order
		assert!(buffer.add_commit(CommitVersion(11), CowVec::new(vec![]),  1));
		assert!(!buffer.add_commit(CommitVersion(12), CowVec::new(vec![]),  2));

		let ready = buffer.drain_ready();
		assert_eq!(ready.len(), 2);
		assert_eq!(ready[0].version, 11);
		assert_eq!(ready[1].version, 12);
		assert_eq!(buffer.next_expected.unwrap(), 13);
	}

	#[test]
	fn test_out_of_order_commits() {
		let mut buffer = CommitBuffer::new();

		// Establish baseline with version 1
		assert!(buffer.add_commit(CommitVersion(1), CowVec::new(vec![]),  0));
		buffer.drain_ready();

		// Add commits out of order
		assert!(!buffer.add_commit(CommitVersion(3), CowVec::new(vec![]),  3));
		assert!(!buffer.add_commit(CommitVersion(5), CowVec::new(vec![]),  5));
		assert!(buffer.add_commit(CommitVersion(2), CowVec::new(vec![]),  2));
		assert!(!buffer.add_commit(CommitVersion(4), CowVec::new(vec![]),  4));

		// Should drain 2, 3, 4, 5 in order
		let ready = buffer.drain_ready();
		assert_eq!(ready.len(), 4);
		assert_eq!(ready[0].version, 2);
		assert_eq!(ready[1].version, 3);
		assert_eq!(ready[2].version, 4);
		assert_eq!(ready[3].version, 5);
		assert_eq!(buffer.next_expected.unwrap(), 6);
	}

	#[test]
	fn test_gap_in_versions() {
		let mut buffer = CommitBuffer::new();

		// Establish baseline
		assert!(buffer.add_commit(CommitVersion(1), CowVec::new(vec![]),  0));
		buffer.drain_ready();

		// Add with a gap
		assert!(!buffer.add_commit(CommitVersion(3), CowVec::new(vec![]),  3));
		assert!(!buffer.add_commit(CommitVersion(4), CowVec::new(vec![]),  4));
		assert!(!buffer.add_commit(CommitVersion(5), CowVec::new(vec![]),  5));

		// Nothing should drain yet - waiting for version 2
		let ready = buffer.drain_ready();
		assert_eq!(ready.len(), 0);
		assert_eq!(buffer.buffer.len(), 3);

		// Add the missing version
		assert!(buffer.add_commit(CommitVersion(2), CowVec::new(vec![]),  2));

		// Now all should drain
		let ready = buffer.drain_ready();
		assert_eq!(ready.len(), 4);
		assert_eq!(ready[0].version, 2);
		assert_eq!(ready[1].version, 3);
		assert_eq!(ready[2].version, 4);
		assert_eq!(ready[3].version, 5);
	}
}
