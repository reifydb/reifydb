use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use dashmap::DashMap;
use mpsc::Sender;
use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	runtime::SharedRuntime,
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::Result;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::{
	coordinator,
	coordinator::CoordinatorMessage,
	handle::TaskHandle,
	registry::{TaskEntry, TaskRegistry},
	task::ScheduledTask,
};

/// Task scheduler subsystem
pub struct TaskSubsystem {
	/// Whether the subsystem is running
	running: AtomicBool,
	/// Handle to interact with the task scheduler
	handle: Option<TaskHandle>,
	/// Sender to the coordinator
	coordinator_tx: Option<Sender<CoordinatorMessage>>,
	/// Shared runtime for spawning tasks
	runtime: SharedRuntime,
	/// Database engine for task execution
	engine: StandardEngine,
	/// Registry of scheduled tasks
	registry: TaskRegistry,
	/// Initial tasks to register on startup
	initial_tasks: Vec<ScheduledTask>,
}

impl TaskSubsystem {
	/// Create a new task subsystem
	#[instrument(name = "task::subsystem::new", level = "debug", skip(ioc, initial_tasks))]
	pub fn new(ioc: &IocContainer, initial_tasks: Vec<ScheduledTask>) -> Self {
		let runtime = ioc.resolve::<SharedRuntime>().expect("SharedRuntime not registered in IoC");
		let engine = ioc.resolve::<StandardEngine>().expect("StandardEngine not registered in IoC");
		let registry = Arc::new(DashMap::new());

		Self {
			running: AtomicBool::new(false),
			handle: None,
			coordinator_tx: None,
			runtime,
			engine,
			registry,
			initial_tasks,
		}
	}

	/// Get a handle to interact with the task scheduler
	///
	/// Returns None if the subsystem is not running
	pub fn handle(&self) -> Option<TaskHandle> {
		self.handle.clone()
	}
}

impl Subsystem for TaskSubsystem {
	fn name(&self) -> &'static str {
		"sub-task"
	}

	#[instrument(name = "task::subsystem::start", level = "info", skip(self))]
	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::Acquire) {
			// Already running
			return Ok(());
		}

		tracing::info!("Starting task subsystem");

		// Create coordinator channel
		let (coordinator_tx, coordinator_rx) = mpsc::channel(100);

		// Register initial tasks in the registry
		for task in self.initial_tasks.drain(..) {
			let next_execution = std::time::Instant::now() + task.schedule.initial_delay();
			self.registry.insert(
				task.id,
				TaskEntry {
					task: Arc::new(task),
					next_execution,
				},
			);
		}

		// Create handle
		let handle = TaskHandle::new(self.registry.clone(), coordinator_tx.clone());

		// Spawn coordinator
		let registry = self.registry.clone();
		let runtime = self.runtime.clone();
		let engine = self.engine.clone();

		self.runtime.spawn(async move {
			coordinator::run_coordinator(registry, coordinator_rx, runtime, engine).await;
		});

		// Store handle and coordinator_tx
		self.handle = Some(handle);
		self.coordinator_tx = Some(coordinator_tx);
		self.running.store(true, Ordering::Release);

		tracing::info!("Task subsystem started");

		Ok(())
	}

	#[instrument(name = "task::subsystem::shutdown", level = "info", skip(self))]
	fn shutdown(&mut self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			// Already shutdown
			return Ok(());
		}

		tracing::info!("Shutting down task subsystem");

		// Send shutdown message to coordinator
		if let Some(coordinator_tx) = self.coordinator_tx.take() {
			let _ = coordinator_tx.blocking_send(CoordinatorMessage::Shutdown);
		}

		self.handle = None;

		tracing::info!("Task subsystem shut down");

		Ok(())
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

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

impl HasVersion for TaskSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-task".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Periodic task scheduler subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
