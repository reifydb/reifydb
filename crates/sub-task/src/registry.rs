// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use dashmap::DashMap;
use reifydb_core::interface::catalog::task::TaskId;
use reifydb_runtime::context::clock::Instant;

use crate::task::ScheduledTask;

#[derive(Debug, Clone)]
pub struct TaskEntry {
	pub task: Arc<ScheduledTask>,

	pub next_execution: Instant,
}

pub type TaskRegistry = Arc<DashMap<TaskId, TaskEntry>>;
