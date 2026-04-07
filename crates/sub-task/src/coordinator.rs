// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{cmp::Ordering, collections::BinaryHeap, error::Error, future, io, sync::Arc};

use reifydb_core::interface::catalog::task::TaskId;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{SharedRuntime, context::clock::Instant};
use tokio::{select, sync::mpsc, task::spawn_blocking, time};
use tracing::{debug, error, info};

use crate::{
	context::TaskContext,
	registry::{TaskEntry, TaskRegistry},
	task::{ScheduledTask, TaskExecutor, TaskWork},
};

/// Messages sent to the coordinator task
#[derive(Debug)]
pub enum TaskCoordinatorMessage {
	/// Register a new task
	Register(ScheduledTask),
	/// Unregister a task by ID
	Unregister(TaskId),
	/// A task has completed execution
	TaskCompleted {
		task_id: TaskId,
		completed_at: Instant,
	},
	/// Request immediate shutdown
	Shutdown,
}

/// Entry in the scheduling heap
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeapEntry {
	next_execution: Instant,
	task_id: TaskId,
}

impl Ord for HeapEntry {
	fn cmp(&self, other: &Self) -> Ordering {
		// Reverse ordering to make BinaryHeap a min-heap
		other.next_execution.cmp(&self.next_execution)
	}
}

impl PartialOrd for HeapEntry {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

/// Run the coordinator loop
pub async fn run_coordinator(
	registry: TaskRegistry,
	mut rx: mpsc::Receiver<TaskCoordinatorMessage>,
	runtime: SharedRuntime,
	engine: StandardEngine,
) {
	info!("Task coordinator started");

	// Create a channel for task completion notifications
	let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();

	// Min-heap of tasks ordered by next execution time
	let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

	// Build initial heap from registry
	for entry in registry.iter() {
		heap.push(HeapEntry {
			next_execution: entry.value().next_execution.clone(),
			task_id: *entry.key(),
		});
	}

	loop {
		// Calculate sleep duration until next task
		let sleep_duration = heap.peek().map(|entry| {
			let now = runtime.clock().instant();
			if entry.next_execution > now {
				&entry.next_execution - &now
			} else {
				time::Duration::ZERO
			}
		});

		select! {
		    // Next task is due
		    _ = async {
			match sleep_duration {
			    Some(duration) => time::sleep(duration).await,
			    None => future::pending::<()>().await, // No tasks, wait forever
			}
		    } => {
			// Pop the task from the heap
			if let Some(heap_entry) = heap.pop() {
			    // Get task from registry
			    if let Some(entry) = registry.get(&heap_entry.task_id) {
				let task = entry.task.clone();
				let task_id = heap_entry.task_id;
				let task_name = task.name.clone();

				// Spawn task execution
				spawn_task(
				    task_id,
				    task,
				    runtime.clone(),
				    engine.clone(),
				    completion_tx.clone(),
				);

				debug!("Spawned task: {}", task_name);
			    }
			}
		    }

		    // Handle task completion notifications
		    Some((task_id, completed_at)) = completion_rx.recv() => {
			// Check if task should be rescheduled
			if let Some(mut entry) = registry.get_mut(&task_id) {
			    if let Some(next_exec) = entry.task.schedule.next_execution(completed_at) {
				// Update next execution time
				entry.next_execution = next_exec.clone();

				// Add back to heap
				heap.push(HeapEntry {
				    next_execution: next_exec,
				    task_id,
				});

				debug!("Rescheduled task: {}", entry.task.name);
			    } else {
				// One-shot task, remove from registry
				let task_name = entry.task.name.clone();
				drop(entry); // Release the lock
				registry.remove(&task_id);
				debug!("Completed one-shot task: {}", task_name);
			    }
			}
		    }

		    // Handle coordinator messages
		    Some(msg) = rx.recv() => {
			match msg {
			    TaskCoordinatorMessage::Register(task) => {
				let task_id = task.id;
				let next_execution = runtime.clock().instant() + task.schedule.initial_delay();

				info!("Registering task: {} (id: {})", task.name, task_id);

				// Add to registry
				registry.insert(task_id, TaskEntry {
				    task: Arc::new(task),
				    next_execution: next_execution.clone(),
				});

				// Add to heap
				heap.push(HeapEntry {
				    next_execution,
				    task_id,
				});
			    }

			    TaskCoordinatorMessage::Unregister(task_id) => {
				info!("Unregistering task: {}", task_id);

				// Remove from registry
				registry.remove(&task_id);

				// Rebuild heap (simplest approach for now)
				heap.clear();
				for entry in registry.iter() {
				    heap.push(HeapEntry {
					next_execution: entry.value().next_execution.clone(),
					task_id: *entry.key(),
				    });
				}
			    }

			    TaskCoordinatorMessage::Shutdown => {
				info!("Task coordinator shutting down");
				break;
			    }
			TaskCoordinatorMessage::TaskCompleted{ .. } => {}}
		    }

		    else => {
			// Channel closed, shutdown
			info!("Coordinator channel closed, shutting down");
			break;
		    }
		}
	}

	info!("Task coordinator stopped");
}

/// Spawn a task execution
fn spawn_task(
	task_id: TaskId,
	task: Arc<ScheduledTask>,
	runtime: SharedRuntime,
	engine: StandardEngine,
	completion_tx: mpsc::UnboundedSender<(TaskId, Instant)>,
) {
	let task_name = task.name.clone();
	let executor = task.executor;
	let work = task.work.clone();
	let runtime_clone = runtime.clone();

	runtime.spawn(async move {
		let runtime = runtime_clone;
		let start = runtime.clock().instant();
		let ctx = TaskContext::new(engine);

		// Execute the work
		let result = match (&work, executor) {
			(TaskWork::Sync(f), TaskExecutor::ComputePool) => {
				let f = f.clone();
				let ctx_clone = ctx.clone();
				spawn_blocking(move || f(ctx_clone))
					.await
					.map_err(|e| Box::new(e) as Box<dyn Error + Send>)
					.and_then(|r| r)
			}
			(TaskWork::Async(f), TaskExecutor::Tokio) => f(ctx).await,
			(TaskWork::Sync(_), TaskExecutor::Tokio) => Err(Box::new(io::Error::new(
				io::ErrorKind::InvalidInput,
				"Sync work cannot be executed on Tokio executor",
			)) as Box<dyn Error + Send>),
			(TaskWork::Async(_), TaskExecutor::ComputePool) => Err(Box::new(io::Error::new(
				io::ErrorKind::InvalidInput,
				"Async work cannot be executed on ComputePool executor",
			)) as Box<dyn Error + Send>),
		};

		let duration = start.elapsed();
		let completed_at = runtime.clock().instant();

		// Log result
		match result {
			Ok(()) => {
				debug!("Task '{}' completed successfully in {:?}", task_name, duration);
			}
			Err(e) => {
				error!("Task '{}' failed after {:?}: {}", task_name, duration, e);
			}
		}

		// Send completion notification to coordinator
		let _ = completion_tx.send((task_id, completed_at));
	});
}
