// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::common::CommitVersion;

define_event! {
	/// Event emitted when stats have been processed up to a version.
	/// Emitted once per batch, not per individual version.
	pub struct StatsProcessedEvent {
		pub up_to: CommitVersion,
	}
}
