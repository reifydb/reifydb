use tokio::sync::mpsc;

use crate::{
	coordinator::CoordinatorMessage,
	registry::{TaskInfo, TaskRegistry},
	task::{ScheduledTask, TaskId},
};

/// Handle for interacting with the task scheduler at runtime
#[derive(Clone)]
pub struct TaskHandle {
	registry: TaskRegistry,
	coordinator_tx: mpsc::Sender<CoordinatorMessage>,
}

impl TaskHandle {
	pub(crate) fn new(registry: TaskRegistry, coordinator_tx: mpsc::Sender<CoordinatorMessage>) -> Self {
		Self {
			registry,
			coordinator_tx,
		}
	}

	pub async fn register_task(&self, task: ScheduledTask) -> Result<TaskId, String> {
		let task_id = task.id;

		self.coordinator_tx
			.send(CoordinatorMessage::Register(task))
			.await
			.map_err(|e| format!("Failed to register task: {}", e))?;

		Ok(task_id)
	}

	pub async fn unregister_task(&self, task_id: TaskId) -> Result<(), String> {
		self.coordinator_tx
			.send(CoordinatorMessage::Unregister(task_id))
			.await
			.map_err(|e| format!("Failed to unregister task: {}", e))?;

		Ok(())
	}

	pub fn list_tasks(&self) -> Vec<TaskInfo> {
		self.registry.iter().map(|entry| TaskInfo::from_entry(*entry.key(), entry.value())).collect()
	}

	pub fn get_task_info(&self, task_id: TaskId) -> Option<TaskInfo> {
		self.registry.get(&task_id).map(|entry| TaskInfo::from_entry(task_id, entry.value()))
	}
}
