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

use reifydb_core::{
	Result, event::lifecycle::OnStartEvent, interface::WithEventBus, log_debug, log_error, log_timed_trace,
	log_warn,
};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::HealthStatus;
#[cfg(feature = "sub_worker")]
use reifydb_sub_api::Scheduler;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowSubsystem;
#[cfg(feature = "sub_server")]
use reifydb_sub_server::ServerSubsystem;

use crate::{
	boot::Bootloader,
	defaults::{GRACEFUL_SHSVTDOWN_TIMEOSVT, HEALTH_CHECK_INTERVAL, MAX_STARTUP_TIME},
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
			graceful_shutdown_timeout: GRACEFUL_SHSVTDOWN_TIMEOSVT,
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
	#[cfg(feature = "sub_worker")]
	scheduler: Option<Arc<dyn Scheduler>>,
}

impl Database {
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
		#[cfg(feature = "sub_worker")] scheduler: Option<Arc<dyn Scheduler>>,
	) -> Self {
		Self {
			engine: engine.clone(),
			bootloader: Bootloader::new(engine),
			subsystems: subsystem_manager,
			config,
			health_monitor,
			running: false,
			#[cfg(feature = "sub_worker")]
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

	pub fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(()); // Already running
		}

		log_timed_trace!("Bootloader setup", { self.bootloader.load()? });

		log_debug!("Starting system with {} subsystems", self.subsystem_count());

		log_timed_trace!("Database initialization", {
			self.engine.event_bus().emit(OnStartEvent {});
		});

		// Start all subsystems
		match log_timed_trace!("Starting all subsystems", {
			let result = self.subsystems.start_all(self.config.max_startup_time);
			result
		}) {
			Ok(()) => {
				self.running = true;
				log_debug!("System started successfully");
				self.update_health_monitoring();
				Ok(())
			}
			Err(e) => {
				log_error!("System startup failed: {}", e);
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

	pub fn stop(&mut self) -> Result<()> {
		if !self.running {
			return Ok(()); // Already stopped
		}

		log_debug!("Stopping system gracefully");

		// Stop all subsystems
		let result = self.subsystems.stop_all(self.config.graceful_shutdown_timeout);

		self.running = false;

		match result {
			Ok(()) => {
				log_debug!("System stopped successfully");
				self.health_monitor.update_component_health(
					"system".to_string(),
					HealthStatus::Healthy,
					false,
				);
				Ok(())
			}
			Err(e) => {
				log_warn!("System shutdown completed with errors: {}", e);
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

	#[cfg(feature = "sub_worker")]
	pub fn scheduler(&self) -> Option<Arc<dyn Scheduler>> {
		self.scheduler.clone()
	}

	pub fn await_signal(&self) -> Result<()> {
		static RUNNING: AtomicBool = AtomicBool::new(true);

		extern "C" fn handle_signal(sig: libc::c_int) {
			let signal_name = match sig {
				libc::SIGINT => "SIGINT (Ctrl+C)",
				libc::SIGTERM => "SIGTERM",
				libc::SIGQUIT => "SIGQUIT",
				libc::SIGHUP => "SIGHUP",
				_ => "Unknown signal",
			};
			log_debug!("Received {}, signaling shutdown...", signal_name);
			RUNNING.store(false, Ordering::SeqCst);
		}

		unsafe {
			libc::signal(libc::SIGINT, handle_signal as libc::sighandler_t);
			libc::signal(libc::SIGTERM, handle_signal as libc::sighandler_t);
			libc::signal(libc::SIGQUIT, handle_signal as libc::sighandler_t);
			libc::signal(libc::SIGHUP, handle_signal as libc::sighandler_t);
		}

		log_debug!("Waiting for termination signal...");
		while RUNNING.load(Ordering::SeqCst) {
			std::thread::sleep(Duration::from_millis(100));
		}

		Ok(())
	}

	pub fn start_and_await_signal(&mut self) -> Result<()> {
		self.start()?;
		log_debug!("Database started, waiting for termination signal...");

		self.await_signal()?;

		log_debug!("Signal received, shutting down database...");
		self.stop()?;

		Ok(())
	}
}

impl Drop for Database {
	fn drop(&mut self) {
		if self.running {
			log_warn!("System being dropped while running, attempting graceful shutdown");
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

	#[cfg(feature = "sub_worker")]
	fn scheduler(&self) -> Option<Arc<dyn Scheduler>> {
		self.scheduler.clone()
	}
}
