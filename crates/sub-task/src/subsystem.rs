// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
};

use dashmap::DashMap;
use mpsc::Sender;
use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{context::clock::Clock, shutdown::Shutdown, sync::mutex::Mutex};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use tokio::{runtime::Handle, sync::mpsc, task::JoinHandle};
use tracing::{info, instrument};

use crate::{
	coordinator,
	coordinator::TaskCoordinatorMessage,
	handle::TaskHandle,
	registry::{TaskEntry, TaskRegistry},
	task::ScheduledTask,
};

pub struct TaskSubsystem {
	running: AtomicBool,

	handle: Mutex<Option<TaskHandle>>,

	coordinator_tx: Mutex<Option<Sender<TaskCoordinatorMessage>>>,

	coordinator_handle: Mutex<Option<JoinHandle<()>>>,

	handle_tokio: Handle,
}

impl TaskSubsystem {
	#[instrument(name = "task::subsystem::new", level = "debug", skip(ioc, initial_tasks))]
	pub fn new(ioc: &IocContainer, initial_tasks: Vec<ScheduledTask>) -> Self {
		let clock = ioc.resolve::<Clock>().expect("Clock not registered in IoC");
		let handle_tokio = ioc.resolve::<Handle>().expect("tokio::runtime::Handle not registered in IoC");
		let engine = ioc.resolve::<StandardEngine>().expect("StandardEngine not registered in IoC");
		let registry: TaskRegistry = Arc::new(DashMap::new());

		info!("Starting task subsystem");

		let (coordinator_tx, coordinator_rx) = mpsc::channel(100);

		for task in initial_tasks {
			let next_execution = clock.instant() + task.schedule.initial_delay();
			registry.insert(
				task.id,
				TaskEntry {
					task: Arc::new(task),
					next_execution,
				},
			);
		}

		let handle = TaskHandle::new(registry.clone(), coordinator_tx.clone());

		let coordinator_handle = {
			let registry = registry.clone();
			let clock = clock.clone();
			let engine = engine.clone();
			let coordinator_tokio = handle_tokio.clone();
			handle_tokio.spawn(async move {
				coordinator::run_coordinator(
					registry,
					coordinator_rx,
					clock,
					coordinator_tokio,
					engine,
				)
				.await;
			})
		};

		info!("Task subsystem started");

		Self {
			running: AtomicBool::new(true),
			handle: Mutex::new(Some(handle)),
			coordinator_tx: Mutex::new(Some(coordinator_tx)),
			coordinator_handle: Mutex::new(Some(coordinator_handle)),
			handle_tokio,
		}
	}

	pub fn handle(&self) -> Option<TaskHandle> {
		self.handle.lock().clone()
	}
}

impl Shutdown for TaskSubsystem {
	#[instrument(name = "task::subsystem::shutdown", level = "debug", skip(self))]
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}

		info!("Shutting down task subsystem");

		let coordinator_tx = self.coordinator_tx.lock().take();
		let coordinator_handle = self.coordinator_handle.lock().take();
		let handle_tokio = self.handle_tokio.clone();
		let worker = thread::spawn(move || {
			if let Some(coordinator_tx) = coordinator_tx {
				let _ = coordinator_tx.blocking_send(TaskCoordinatorMessage::Shutdown);
			}
			if let Some(join_handle) = coordinator_handle {
				let _ = handle_tokio.block_on(join_handle);
			}
		});
		let _ = worker.join();

		*self.handle.lock() = None;

		info!("Task subsystem shut down");
	}
}

impl Subsystem for TaskSubsystem {
	fn name(&self) -> &'static str {
		"sub-task"
	}

	#[instrument(name = "task::subsystem::is_running", level = "trace", skip(self))]
	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	#[instrument(name = "task::subsystem::health_status", level = "debug", skip(self))]
	fn health_status(&self) -> HealthStatus {
		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}

impl HasVersion for TaskSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Periodic task scheduler subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
