// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::common::CommitVersion;

/// Retention strategy for managing MVCC version cleanup
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RetentionStrategy {
	/// Keep all versions forever (default)
	#[default]
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

impl RetentionStrategy {
	/// Check if a version should be retained based on the strategy
	pub fn should_retain(
		&self,
		_version: CommitVersion,
		_current_version: CommitVersion,
		version_count: u64,
	) -> bool {
		match self {
			RetentionStrategy::KeepForever => true,

			RetentionStrategy::KeepVersions {
				count,
				..
			} => {
				// Keep if within the last N versions
				version_count <= *count
			}
		}
	}

	/// Get the cleanup mode for this strategy
	pub fn cleanup_mode(&self) -> Option<CleanupMode> {
		match self {
			RetentionStrategy::KeepForever => None,
			RetentionStrategy::KeepVersions {
				cleanup_mode,
				..
			} => Some(*cleanup_mode),
		}
	}
}
