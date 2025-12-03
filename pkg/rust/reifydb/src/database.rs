// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_core::{Result, event::lifecycle::OnStartEvent, interface::WithEventBus};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{HealthStatus, SchedulerService};
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowSubsystem;
#[cfg(feature = "sub_server")]
use reifydb_sub_server::ServerSubsystem;
use reifydb_sub_worker::WorkerSubsystem;
use tracing::{debug, error, instrument, trace, warn};

use crate::{
	boot::Bootloader,
	defaults::{GRACEFUL_SHUTDOWN_TIMEOUT, HEALTH_CHECK_INTERVAL, MAX_STARTUP_TIME},
	health::{ComponentHealth, HealthMonitor},
	session::{CommandSession, IntoCommandSession, IntoQuerySession, QuerySession, Session},
	subsystem::Subsystems,
};

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
	pub graceful_shutdown_timeout: Duration,
	pub health_check_interval: Duration,
	pub max_startup_time: Duration,
}

impl DatabaseConfig {
	pub fn new() -> Self {
		Self {
			graceful_shutdown_timeout: GRACEFUL_SHUTDOWN_TIMEOUT,
			health_check_interval: HEALTH_CHECK_INTERVAL,
			max_startup_time: MAX_STARTUP_TIME,
		}
	}

	pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
		self.graceful_shutdown_timeout = timeout;
		self
	}

	pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
		self.health_check_interval = interval;
		self
	}

	pub fn with_max_startup_time(mut self, timeout: Duration) -> Self {
		self.max_startup_time = timeout;
		self
	}
}

impl Default for DatabaseConfig {
	fn default() -> Self {
		Self::new()
	}
}

pub struct Database {
	config: DatabaseConfig,
	engine: StandardEngine,
	bootloader: Bootloader,
	subsystems: Subsystems,
	health_monitor: Arc<HealthMonitor>,
	running: bool,
	scheduler: Option<SchedulerService>,
}

impl Database {
	pub fn sub_worker(&self) -> Option<&WorkerSubsystem> {
		self.subsystem::<WorkerSubsystem>()
	}

	#[cfg(feature = "sub_flow")]
	pub fn sub_flow(&self) -> Option<&FlowSubsystem> {
		self.subsystem::<FlowSubsystem>()
	}

	#[cfg(feature = "sub_server")]
	pub fn sub_server(&self) -> Option<&ServerSubsystem> {
		self.subsystems.get::<ServerSubsystem>()
	}
}

impl Database {
	pub(crate) fn new(
		engine: StandardEngine,
		subsystem_manager: Subsystems,
		config: DatabaseConfig,
		health_monitor: Arc<HealthMonitor>,
		scheduler: Option<SchedulerService>,
	) -> Self {
		Self {
			engine: engine.clone(),
			bootloader: Bootloader::new(engine),
			subsystems: subsystem_manager,
			config,
			health_monitor,
			running: false,
			scheduler,
		}
	}

	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	pub fn config(&self) -> &DatabaseConfig {
		&self.config
	}

	pub fn is_running(&self) -> bool {
		self.running
	}

	pub fn subsystem_count(&self) -> usize {
		self.subsystems.subsystem_count()
	}

	#[instrument(level = "info", skip(self))]
	pub fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(()); // Already running
		}

		trace!("Bootloader setup");
		self.bootloader.load()?;

		debug!("Starting system with {} subsystems", self.subsystem_count());

		trace!("Database initialization");
		self.engine.event_bus().emit(OnStartEvent {});

		// Start all subsystems
		trace!("Starting all subsystems");
		match self.subsystems.start_all(self.config.max_startup_time) {
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

	#[instrument(level = "info", skip(self))]
	pub fn stop(&mut self) -> Result<()> {
		if !self.running {
			return Ok(()); // Already stopped
		}

		debug!("Stopping system gracefully");

		// Stop all subsystems
		let result = self.subsystems.stop_all(self.config.graceful_shutdown_timeout);

		self.running = false;

		match result {
			Ok(()) => {
				debug!("System stopped successfully");
				self.health_monitor.update_component_health(
					"system".to_string(),
					HealthStatus::Healthy,
					false,
				);
				Ok(())
			}
			Err(e) => {
				warn!("System shutdown completed with errors: {}", e);
				self.health_monitor.update_component_health(
					"system".to_string(),
					HealthStatus::Warning {
						description: format!("Shutdown completed with errors: {}", e),
					},
					false,
				);
				Err(e)
			}
		}
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

	pub fn get_stale_components(&self) -> Vec<String> {
		self.health_monitor.get_stale_components(self.config.health_check_interval * 2)
	}

	pub fn subsystem<S: 'static>(&self) -> Option<&S> {
		self.subsystems.get::<S>()
	}

	pub fn scheduler(&self) -> Option<SchedulerService> {
		self.scheduler.clone()
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

	fn scheduler(&self) -> Option<SchedulerService> {
		self.scheduler.clone()
	}
}
