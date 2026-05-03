// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
