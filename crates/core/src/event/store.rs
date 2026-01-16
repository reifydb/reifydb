// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{common::CommitVersion, impl_event};

/// Event emitted when stats have been processed up to a version.
/// Emitted once per batch, not per individual version.
#[derive(Debug, Clone)]
pub struct StatsProcessed {
	pub up_to: CommitVersion,
}

impl_event!(StatsProcessed);
