// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::common::CommitVersion;
use tracing::{debug, instrument};

use super::CommittedWindow;

/// Low water mark: minimum number of windows to retain after cleanup
const WINDOW_LOW_WATER_MARK: usize = 10;

/// Performs cleanup of old windows.
///
/// Evicts oldest-first while two conditions hold:
/// 1. `time_windows.len() > WINDOW_LOW_WATER_MARK`, and
/// 2. the candidate window's `max_version <= safe_evict_below`, meaning no in-flight transaction's read snapshot could
///    still need its conflict history. `safe_evict_below` is the query watermark's `done_until`: every read snapshot at
///    or below it has already terminated, so any window whose contents fall entirely within that range is unreachable.
///
/// Each evicted window's `max_version` advances `evicted_up_through`, so a
/// subsequent commit whose read-version preceded the evicted contents is
/// rejected with `TooOld`. Stopping at the first window that overlaps an
/// active reader is what prevents that error under normal load: as long as
/// the watermark is advancing, eviction can only ever bump
/// `evicted_up_through` to a version that is already done.
#[instrument(name = "transaction::oracle::cleanup", level = "debug", skip(time_windows, evicted_up_through), fields(window_count = time_windows.len(), safe_evict_below = %safe_evict_below))]
pub(super) fn cleanup_old_windows(
	time_windows: &mut BTreeMap<CommitVersion, CommittedWindow>,
	evicted_up_through: &mut CommitVersion,
	safe_evict_below: CommitVersion,
) {
	let mut removed = 0usize;
	while time_windows.len() > WINDOW_LOW_WATER_MARK {
		let Some((&start, window)) = time_windows.iter().next() else {
			break;
		};
		if window.max_version() > safe_evict_below {
			// Some active reader's snapshot is at or below this
			// window's max_version, so its conflict set is still
			// reachable. Stop here; we'll retry on a future commit
			// once the watermark advances.
			break;
		}
		let Some(evicted) = time_windows.remove(&start) else {
			break;
		};
		*evicted_up_through = (*evicted_up_through).max(evicted.max_version());
		removed += 1;
	}

	if removed > 0 {
		debug!(removed, "evicted old windows");
	}
}
