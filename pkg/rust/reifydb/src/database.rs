// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_core::interface::auth::Identity;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::HealthStatus;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::subsystem::FlowSubsystem;
#[cfg(feature = "sub_server_http")]
use reifydb_sub_server_http::subsystem::HttpSubsystem;
#[cfg(feature = "sub_server_ws")]
use reifydb_sub_server_ws::subsystem::WsSubsystem;
use reifydb_sub_task::{handle::TaskHandle, subsystem::TaskSubsystem};
use reifydb_type::{Result, params::Params, value::frame::frame::Frame};
use tracing::{debug, error, instrument, warn};

use crate::{
	health::{ComponentHealth, HealthMonitor},
	session::{CommandSession, IntoCommandSession, IntoQuerySession, QuerySession, Session},
	subsystem::Subsystems,
};

pub struct Database {
	engine: StandardEngine,
	subsystems: Subsystems,
	health_monitor: Arc<HealthMonitor>,
	shared_runtime: SharedRuntime,
	running: bool,
}

impl Database {
	#[cfg(feature = "sub_flow")]
	pub fn sub_flow(&self) -> Option<&FlowSubsystem> {
		self.subsystem::<FlowSubsystem>()
	}

	#[cfg(feature = "sub_server_http")]
	pub fn sub_server_http(&self) -> Option<&HttpSubsystem> {
		self.subsystem::<HttpSubsystem>()
	}

	#[cfg(feature = "sub_server_ws")]
	pub fn sub_server_ws(&self) -> Option<&WsSubsystem> {
		self.subsystem::<WsSubsystem>()
	}

	/// Get a handle to the task scheduler subsystem
	///
	/// Returns None if the task subsystem is not registered or not running
	pub fn task_handle(&self) -> Option<TaskHandle> {
		self.subsystem::<TaskSubsystem>().and_then(|subsystem| subsystem.handle())
	}
}

impl Database {
	pub(crate) fn new(
		engine: StandardEngine,
		subsystem_manager: Subsystems,
		health_monitor: Arc<HealthMonitor>,
		shared_runtime: SharedRuntime,
	) -> Self {
		Self {
			engine: engine.clone(),
			subsystems: subsystem_manager,
			health_monitor,
			shared_runtime,
			running: false,
		}
	}

	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	pub fn shared_runtime(&self) -> &SharedRuntime {
		&self.shared_runtime
	}

	pub fn is_running(&self) -> bool {
		self.running
	}

	pub fn subsystem_count(&self) -> usize {
		self.subsystems.subsystem_count()
	}

	#[instrument(name = "api::database::start", level = "debug", skip(self))]
	pub fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(()); // Already running
		}

		debug!("Starting system with {} subsystems", self.subsystem_count());

		// Start all subsystems
		match self.subsystems.start_all() {
			Ok(()) => {
				self.running = true;
				debug!("System started successfully");
				self.update_health_monitoring();
				Ok(())
			}
			Err(e) => {
				error!("System startup failed: {}", e);
				// Update system health to reflect failure
				self.health_monitor.update_component_health(
					"system".to_string(),
					HealthStatus::Failed {
						description: format!("Startup failed: {}", e),
					},
					false,
				);
				Err(e)
			}
		}
	}

	#[instrument(name = "api::database::stop", level = "debug", skip(self))]
	pub fn stop(&mut self) -> Result<()> {
		if !self.running {
			return Ok(()); // Already stopped
		}

		debug!("Stopping system gracefully");

		// Stop all subsystems
		self.subsystems.stop_all()?;
		self.running = false;
		debug!("System stopped successfully");
		self.health_monitor.update_component_health("system".to_string(), HealthStatus::Healthy, false);
		Ok(())
	}

	pub fn health_status(&self) -> HealthStatus {
		self.health_monitor.get_system_health()
	}

	pub fn get_all_component_health(&self) -> HashMap<String, ComponentHealth> {
		self.health_monitor.get_all_health()
	}

	pub fn update_health_monitoring(&mut self) {
		// Update subsystem health
		self.subsystems.update_health_monitoring();

		// Update system health
		let system_health = if self.running {
			self.health_monitor.get_system_health()
		} else {
			HealthStatus::Healthy
		};

		self.health_monitor.update_component_health("system".to_string(), system_health, self.running);
	}

	pub fn get_subsystem_names(&self) -> Vec<String> {
		self.subsystems.get_subsystem_names()
	}

	pub fn subsystem<S: 'static>(&self) -> Option<&S> {
		self.subsystems.get::<S>()
	}

	/// Execute a transactional command as root user.
	pub fn command_as_root(&self, rql: &str, params: impl Into<Params>) -> reifydb_type::Result<Vec<Frame>> {
		let identity = Identity::root();
		self.engine.command_as(&identity, rql, params.into())
	}

	/// Execute a read-only query as root user.
	pub fn query_as_root(&self, rql: &str, params: impl Into<Params>) -> reifydb_type::Result<Vec<Frame>> {
		let identity = Identity::root();
		self.engine.query_as(&identity, rql, params.into())
	}

	pub fn await_signal(&self) -> Result<()> {
		self.await_signal_with_shutdown(|| Ok(()))
	}

	pub fn await_signal_with_shutdown<F>(&self, on_shutdown: F) -> Result<()>
	where
		F: FnOnce() -> Result<()>,
	{
		static RUNNING: AtomicBool = AtomicBool::new(true);
		static SIGNAL_RECEIVED: AtomicBool = AtomicBool::new(false);

		extern "C" fn handle_signal(_sig: libc::c_int) {
			// SAFETY: Only async-signal-safe operations are allowed here.
			// We only use atomic operations, which are signal-safe.
			RUNNING.store(false, Ordering::SeqCst);
			SIGNAL_RECEIVED.store(true, Ordering::SeqCst);
		}

		unsafe {
			libc::signal(libc::SIGINT, handle_signal as libc::sighandler_t);
			libc::signal(libc::SIGTERM, handle_signal as libc::sighandler_t);
			libc::signal(libc::SIGQUIT, handle_signal as libc::sighandler_t);
			libc::signal(libc::SIGHUP, handle_signal as libc::sighandler_t);
		}

		debug!("Waiting for termination signal...");
		while RUNNING.load(Ordering::SeqCst) {
			std::thread::sleep(Duration::from_millis(100));

			// Log the signal reception outside the signal handler
			if SIGNAL_RECEIVED.load(Ordering::SeqCst) {
				debug!("Received termination signal, initiating shutdown...");
				break;
			}
		}

		on_shutdown()?;

		Ok(())
	}

	pub fn start_and_await_signal(&mut self) -> Result<()> {
		self.start_and_await_signal_with_shutdown(|| Ok(()))
	}

	pub fn start_and_await_signal_with_shutdown<F>(&mut self, on_shutdown: F) -> Result<()>
	where
		F: FnOnce() -> Result<()>,
	{
		self.start()?;
		debug!("Database started, waiting for termination signal...");

		self.await_signal()?;

		debug!("Signal received, running shutdown handler...");
		on_shutdown()?;

		debug!("Shutdown handler completed, shutting down database...");
		self.stop()?;

		Ok(())
	}
}

impl Drop for Database {
	fn drop(&mut self) {
		if self.running {
			warn!("System being dropped while running, attempting graceful shutdown");
			let _ = self.stop();
		}
	}
}

impl Session for Database {
	fn command_session(&self, session: impl IntoCommandSession) -> Result<CommandSession> {
		session.into_command_session(self.engine.clone())
	}

	fn query_session(&self, session: impl IntoQuerySession) -> Result<QuerySession> {
		session.into_query_session(self.engine.clone())
	}
}
