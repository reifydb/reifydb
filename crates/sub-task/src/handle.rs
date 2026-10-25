// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::task::TaskId;
use tokio::sync::mpsc;

use crate::{coordinator::TaskCoordinatorMessage, task::ScheduledTask};

#[derive(Clone)]
pub struct TaskHandle {
	coordinator_tx: mpsc::Sender<TaskCoordinatorMessage>,
}

impl TaskHandle {
	pub(crate) fn new(coordinator_tx: mpsc::Sender<TaskCoordinatorMessage>) -> Self {
		Self {
			coordinator_tx,
		}
	}

	pub async fn register_task(&self, task: ScheduledTask) -> Result<TaskId, String> {
		let task_id = task.id;

		self.coordinator_tx
			.send(TaskCoordinatorMessage::Register(task))
			.await
			.map_err(|e| format!("Failed to register task: {}", e))?;

		Ok(task_id)
	}

	pub async fn unregister_task(&self, task_id: TaskId) -> Result<(), String> {
		self.coordinator_tx
			.send(TaskCoordinatorMessage::Unregister(task_id))
			.await
			.map_err(|e| format!("Failed to unregister task: {}", e))?;

		Ok(())
	}
}
