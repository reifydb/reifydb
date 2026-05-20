// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::common::CommitVersion;

define_event! {


	pub struct StatsProcessedEvent {
		pub up_to: CommitVersion,
	}
}
