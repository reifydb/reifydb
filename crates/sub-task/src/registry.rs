// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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

#[derive(Debug, Clone)]
pub struct TaskInfo {
	pub id: TaskId,
	pub name: String,
	pub next_execution: Instant,
}

impl TaskInfo {
	pub fn from_entry(id: TaskId, entry: &TaskEntry) -> Self {
		Self {
			id,
			name: entry.task.name.clone(),
			next_execution: entry.next_execution.clone(),
		}
	}
}
