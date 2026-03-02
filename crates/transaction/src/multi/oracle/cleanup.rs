// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cleanup functions for the oracle's time windows.
//!
//! This module provides cleanup functionality that removes old time windows
//! to prevent unbounded growth.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey};
use tracing::{debug, instrument};

use super::CommittedWindow;

/// Low water mark: minimum number of windows to retain after cleanup
const WINDOW_LOW_WATER_MARK: usize = 10;

/// Performs cleanup of old windows
///
/// This function removes old time windows that exceed the maximum count,
/// keeping only the most recent windows. It also updates the key-to-windows
/// index accordingly.
#[instrument(name = "transaction::oracle::cleanup", level = "debug", skip(time_windows, key_to_windows, evicted_up_through), fields(window_count = time_windows.len()))]
pub(super) fn cleanup_old_windows(
	time_windows: &mut BTreeMap<CommitVersion, CommittedWindow>,
	key_to_windows: &mut HashMap<EncodedKey, BTreeSet<CommitVersion>>,
	evicted_up_through: &mut CommitVersion,
) {
	if time_windows.len() <= WINDOW_LOW_WATER_MARK {
		return;
	}

	// Determine how many windows to remove
	let windows_to_remove = time_windows.len() - WINDOW_LOW_WATER_MARK;
	let old_windows: Vec<CommitVersion> = time_windows.keys().take(windows_to_remove).cloned().collect();

	debug!(windows_to_remove = old_windows.len(), "removing old windows");

	// Remove old windows and update key index
	for window_version in old_windows {
		if let Some(window) = time_windows.remove(&window_version) {
			*evicted_up_through = (*evicted_up_through).max(window.max_version());
			// Remove this window from key index
			for key in window.get_modified_keys() {
				if let Some(window_set) = key_to_windows.get_mut(key) {
					window_set.remove(&window_version);
					if window_set.is_empty() {
						key_to_windows.remove(key);
					}
				}
			}
		}
	}

	key_to_windows.shrink_to_fit();
}
