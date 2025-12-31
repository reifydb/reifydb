// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::common::CommitVersion;

/// Retention policy for managing MVCC version cleanup
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionPolicy {
	/// Keep all versions forever (default)
	KeepForever,

	/// Keep only the last N versions
	KeepVersions {
		count: u64,
		cleanup_mode: CleanupMode,
	},
}

/// Cleanup mode determines how old versions are removed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CleanupMode {
	/// Create tombstones and CDC entries (only for non-deleted keys)
	/// Simulates user deletion - maintains audit trail
	Delete,

	/// Silent removal from storage (works on both live and tombstoned keys)
	/// No CDC entries, no tombstones - direct storage cleanup
	Drop,
}

/// Action to take during cleanup
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupAction {
	/// Create tombstone (mark as deleted)
	Delete,

	/// Remove from storage
	Drop,

	/// Do nothing
	Keep,
}

impl Default for RetentionPolicy {
	fn default() -> Self {
		RetentionPolicy::KeepForever
	}
}

impl RetentionPolicy {
	/// Check if a version should be retained based on the policy
	pub fn should_retain(
		&self,
		_version: CommitVersion,
		_current_version: CommitVersion,
		version_count: u64,
	) -> bool {
		match self {
			RetentionPolicy::KeepForever => true,

			RetentionPolicy::KeepVersions {
				count,
				..
			} => {
				// Keep if within the last N versions
				version_count <= *count
			}
		}
	}

	/// Get the cleanup mode for this policy
	pub fn cleanup_mode(&self) -> Option<CleanupMode> {
		match self {
			RetentionPolicy::KeepForever => None,
			RetentionPolicy::KeepVersions {
				cleanup_mode,
				..
			} => Some(*cleanup_mode),
		}
	}
}
