// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::common::CommitVersion;

define_event! {


	pub struct StatsProcessedEvent {
		pub up_to: CommitVersion,
	}
}
