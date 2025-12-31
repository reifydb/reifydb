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

use futures_util::TryStreamExt;
use reifydb_core::{
	Frame, Result,
	event::lifecycle::OnStartEvent,
	interface::{Identity, Params, WithEventBus},
	stream::StreamError,
};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::HealthStatus;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowSubsystem;
#[cfg(feature = "sub_server_http")]
use reifydb_sub_server_http::HttpSubsystem;
#[cfg(feature = "sub_server_ws")]
use reifydb_sub_server_ws::WsSubsystem;
use tokio::{runtime::Handle, task::block_in_place};
use tracing::{debug, error, instrument, warn};

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
}

impl Database {
	pub(crate) fn new(
		engine: StandardEngine,
		subsystem_manager: Subsystems,
		config: DatabaseConfig,
		health_monitor: Arc<HealthMonitor>,
	) -> Self {
		Self {
			engine: engine.clone(),
			bootloader: Bootloader::new(engine),
			subsystems: subsystem_manager,
			config,
			health_monitor,
			running: false,
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

	#[instrument(name = "api::database::start", level = "info", skip(self))]
	pub async fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(()); // Already running
		}

		self.bootloader.load().await?;

		debug!("Starting system with {} subsystems", self.subsystem_count());

		self.engine.event_bus().emit(OnStartEvent {}).await;

		// Start all subsystems
		match self.subsystems.start_all(self.config.max_startup_time).await {
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

	#[instrument(name = "api::database::stop", level = "info", skip(self))]
	pub async fn stop(&mut self) -> Result<()> {
		if !self.running {
			return Ok(()); // Already stopped
		}

		debug!("Stopping system gracefully");

		// Stop all subsystems
		let result = self.subsystems.stop_all(self.config.graceful_shutdown_timeout).await;

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

	/// Execute a transactional command as root user.
	pub async fn command_as_root(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> std::result::Result<Vec<Frame>, StreamError> {
		let identity = Identity::root();
		self.engine.command_as(&identity, rql, params.into()).try_collect().await
	}

	/// Execute a read-only query as root user.
	pub async fn query_as_root(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> std::result::Result<Vec<Frame>, StreamError> {
		let identity = Identity::root();
		self.engine.query_as(&identity, rql, params.into()).try_collect().await
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

	pub async fn start_and_await_signal(&mut self) -> Result<()> {
		self.start_and_await_signal_with_shutdown(|| Ok(())).await
	}

	pub async fn start_and_await_signal_with_shutdown<F>(&mut self, on_shutdown: F) -> Result<()>
	where
		F: FnOnce() -> Result<()>,
	{
		self.start().await?;
		debug!("Database started, waiting for termination signal...");

		self.await_signal()?;

		debug!("Signal received, running shutdown handler...");
		on_shutdown()?;

		debug!("Shutdown handler completed, shutting down database...");
		self.stop().await?;

		Ok(())
	}
}

impl Drop for Database {
	fn drop(&mut self) {
		if self.running {
			warn!("System being dropped while running, attempting graceful shutdown");
			// Use block_on to call async stop() from sync Drop context
			if let Ok(handle) = Handle::try_current() {
				let _ = block_in_place(|| handle.block_on(self.stop()));
			}
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
