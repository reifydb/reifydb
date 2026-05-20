// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::sync::waiter::WaiterHandle;

#[derive(Debug)]
pub enum WatermarkMessage {
	Begin {
		version: u64,
	},
	Done {
		version: u64,
	},
	WaitFor {
		version: u64,
		waiter: Arc<WaiterHandle>,
	},
}
