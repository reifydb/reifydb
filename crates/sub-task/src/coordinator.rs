// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, collections::BinaryHeap, error::Error, future, io, sync::Arc};

use reifydb_core::interface::catalog::task::TaskId;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	context::clock::{Clock, Instant},
	reifydb_assertions,
};
use tokio::{runtime::Handle, select, sync::mpsc, task::spawn_blocking, time};
use tracing::{debug, error, info};

#[cfg(reifydb_assertions)]
use crate::schedule::Schedule;
use crate::{
	context::TaskContext,
	registry::{TaskEntry, TaskRegistry},
	task::{ScheduledTask, TaskExecutor, TaskWork},
};

#[derive(Debug)]
pub enum TaskCoordinatorMessage {
	Register(ScheduledTask),

	Unregister(TaskId),

	TaskCompleted {
		task_id: TaskId,
		completed_at: Instant,
	},

	Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeapEntry {
	next_execution: Instant,
	task_id: TaskId,
}

impl Ord for HeapEntry {
	fn cmp(&self, other: &Self) -> Ordering {
		other.next_execution.cmp(&self.next_execution)
	}
}

impl PartialOrd for HeapEntry {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

pub async fn run_coordinator(
	registry: TaskRegistry,
	mut rx: mpsc::Receiver<TaskCoordinatorMessage>,
	clock: Clock,
	handle: Handle,
	engine: StandardEngine,
) {
	info!("Task coordinator started");

	let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();

	let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
	seed_heap(&mut heap, &registry);

	loop {
		let sleep_duration = heap.peek().map(|entry| {
			let now = clock.instant();
			if entry.next_execution > now {
				&entry.next_execution - &now
			} else {
				time::Duration::ZERO
			}
		});

		select! {

		    _ = async {
			match sleep_duration {
			    Some(duration) => time::sleep(duration).await,
			    None => future::pending::<()>().await,
			}
		    } => {

			if let Some(heap_entry) = heap.pop()
			    && let Some(entry) = registry.get(&heap_entry.task_id)
			{
				let task = entry.task.clone();
				let task_id = heap_entry.task_id;
				let task_name = task.name.clone();

				spawn_task(
				    task_id,
				    task,
				    clock.clone(),
				    handle.clone(),
				    engine.clone(),
				    completion_tx.clone(),
				);

				debug!("Spawned task: {}", task_name);
			}
		    }


		    Some((task_id, completed_at)) = completion_rx.recv() => {
			handle_completion(&mut heap, &registry, task_id, completed_at);
		    }


		    Some(msg) = rx.recv() => {
			match msg {
			    TaskCoordinatorMessage::Register(task) => {
				handle_register(&mut heap, &registry, &clock, task);
			    }

			    TaskCoordinatorMessage::Unregister(task_id) => {
				handle_unregister(&mut heap, &registry, task_id);
			    }

			    TaskCoordinatorMessage::Shutdown => {
				info!("Task coordinator shutting down");
				break;
			    }
			TaskCoordinatorMessage::TaskCompleted{ .. } => {}}
		    }

		    else => {

			info!("Coordinator channel closed, shutting down");
			break;
		    }
		}
	}

	info!("Task coordinator stopped");
}

#[inline]
fn seed_heap(heap: &mut BinaryHeap<HeapEntry>, registry: &TaskRegistry) {
	for entry in registry.iter() {
		heap.push(HeapEntry {
			next_execution: entry.value().next_execution.clone(),
			task_id: *entry.key(),
		});
	}

	reifydb_assertions! {
		let heap_len = heap.len();
		let registry_len = registry.len();
		assert!(
			heap_len == registry_len,
			"the scheduling heap must hold exactly one entry per registered task after seeding, else a task with no heap slot never gets a fire time (lost task) or a stale slot fires for a missing one (heap={heap_len}, registry={registry_len})"
		);
	}
}

#[inline]
fn handle_completion(
	heap: &mut BinaryHeap<HeapEntry>,
	registry: &TaskRegistry,
	task_id: TaskId,
	completed_at: Instant,
) {
	if let Some(mut entry) = registry.get_mut(&task_id) {
		if let Some(next_exec) = entry.task.schedule.next_execution(completed_at) {
			reifydb_assertions! {
				assert!(
					!matches!(entry.task.schedule, Schedule::Once(_)),
					"a Schedule::Once task entered the reschedule path after completing, so a one-shot task would run repeatedly and duplicate its side effects (task={})",
					entry.task.name
				);
			}

			entry.next_execution = next_exec.clone();

			heap.push(HeapEntry {
				next_execution: next_exec,
				task_id,
			});

			debug!("Rescheduled task: {}", entry.task.name);
		} else {
			let task_name = entry.task.name.clone();
			drop(entry);
			registry.remove(&task_id);
			debug!("Completed one-shot task: {}", task_name);
		}
	}
}

#[inline]
fn handle_register(heap: &mut BinaryHeap<HeapEntry>, registry: &TaskRegistry, clock: &Clock, task: ScheduledTask) {
	let task_id = task.id;
	let next_execution = clock.instant() + task.schedule.initial_delay();

	info!("Registering task: {} (id: {})", task.name, task_id);

	registry.insert(
		task_id,
		TaskEntry {
			task: Arc::new(task),
			next_execution: next_execution.clone(),
		},
	);

	heap.push(HeapEntry {
		next_execution,
		task_id,
	});
}

#[inline]
fn handle_unregister(heap: &mut BinaryHeap<HeapEntry>, registry: &TaskRegistry, task_id: TaskId) {
	info!("Unregistering task: {}", task_id);

	registry.remove(&task_id);

	heap.clear();
	for entry in registry.iter() {
		heap.push(HeapEntry {
			next_execution: entry.value().next_execution.clone(),
			task_id: *entry.key(),
		});
	}

	reifydb_assertions! {
		let heap_len = heap.len();
		let registry_len = registry.len();
		assert!(
			heap_len == registry_len,
			"the scheduling heap must mirror the registry one-for-one after an unregister rebuild, else a surviving task loses its fire slot or a removed task keeps one (heap={heap_len}, registry={registry_len})"
		);
	}
}

fn spawn_task(
	task_id: TaskId,
	task: Arc<ScheduledTask>,
	clock: Clock,
	handle: Handle,
	engine: StandardEngine,
	completion_tx: mpsc::UnboundedSender<(TaskId, Instant)>,
) {
	let task_name = task.name.clone();
	let executor = task.executor;
	let work = task.work.clone();

	handle.spawn(async move {
		let start = clock.instant();
		let ctx = TaskContext::new(engine);

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
		let completed_at = clock.instant();

		match result {
			Ok(()) => {
				debug!("Task '{}' completed successfully in {:?}", task_name, duration);
			}
			Err(e) => {
				error!("Task '{}' failed after {:?}: {}", task_name, duration, e);
			}
		}

		let _ = completion_tx.send((task_id, completed_at));
	});
}
