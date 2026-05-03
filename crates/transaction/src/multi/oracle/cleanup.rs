// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::common::CommitVersion;
use tracing::{debug, instrument};

use super::CommittedWindow;

const WINDOW_LOW_WATER_MARK: usize = 10;

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
