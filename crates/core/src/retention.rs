// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::common::CommitVersion;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RetentionStrategy {
	#[default]
	KeepForever,

	KeepVersions {
		count: u64,
		cleanup_mode: CleanupMode,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CleanupMode {
	Delete,

	Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupAction {
	Delete,

	Drop,

	Keep,
}

impl RetentionStrategy {
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
			} => version_count <= *count,
		}
	}

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
