// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::sleep,
	time::Duration,
};

use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, c_int, sighandler_t, signal};
use reifydb_auth::service::AuthService;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{SharedRuntime, actor::system::ActorSystem};
use reifydb_sub_api::subsystem::HealthStatus;
#[cfg(all(feature = "sub_flow", not(reifydb_single_threaded)))]
use reifydb_sub_flow::subsystem::FlowSubsystem;
#[cfg(all(feature = "sub_server_grpc", not(reifydb_single_threaded)))]
use reifydb_sub_server_grpc::subsystem::GrpcSubsystem;
#[cfg(all(feature = "sub_server_http", not(reifydb_single_threaded)))]
use reifydb_sub_server_http::subsystem::HttpSubsystem;
#[cfg(all(feature = "sub_server_ws", not(reifydb_single_threaded)))]
use reifydb_sub_server_ws::subsystem::WsSubsystem;
#[cfg(not(reifydb_single_threaded))]
use reifydb_sub_task::{handle::TaskHandle, subsystem::TaskSubsystem};
use reifydb_type::{
	Result,
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};
use tracing::{debug, error, instrument, warn};

use crate::{
	Migration,
	boot::Bootloader,
	health::{ComponentHealth, HealthMonitor},
	session::Session,
	subsystem::Subsystems,
};

pub struct Database {
	engine: StandardEngine,
	auth_service: AuthService,
	bootloader: Bootloader,
	subsystems: Subsystems,
	health_monitor: Arc<HealthMonitor>,
	shared_runtime: SharedRuntime,
	actor_system: ActorSystem,
	running: bool,
	migrations: Vec<Migration>,
}

impl Database {
	#[cfg(all(feature = "sub_flow", not(reifydb_single_threaded)))]
	pub fn sub_flow(&self) -> Option<&FlowSubsystem> {
		self.subsystem::<FlowSubsystem>()
	}

	#[cfg(all(feature = "sub_server_grpc", not(reifydb_single_threaded)))]
	pub fn sub_server_grpc(&self) -> Option<&GrpcSubsystem> {
		self.subsystem::<GrpcSubsystem>()
	}

	#[cfg(all(feature = "sub_server_http", not(reifydb_single_threaded)))]
	pub fn sub_server_http(&self) -> Option<&HttpSubsystem> {
		self.subsystem::<HttpSubsystem>()
	}

	#[cfg(all(feature = "sub_server_ws", not(reifydb_single_threaded)))]
	pub fn sub_server_ws(&self) -> Option<&WsSubsystem> {
		self.subsystem::<WsSubsystem>()
	}

	/// Get a handle to the task scheduler subsystem
	///
	/// Returns None if the task subsystem is not registered or not running
	#[cfg(not(reifydb_single_threaded))]
	pub fn task_handle(&self) -> Option<TaskHandle> {
		self.subsystem::<TaskSubsystem>().and_then(|subsystem| subsystem.handle())
	}
}

impl Database {
	pub(crate) fn new(
		engine: StandardEngine,
		auth_service: AuthService,
		subsystem_manager: Subsystems,
		health_monitor: Arc<HealthMonitor>,
		shared_runtime: SharedRuntime,
		actor_system: ActorSystem,
		migrations: Vec<Migration>,
	) -> Self {
		Self {
			engine: engine.clone(),
			auth_service,
			bootloader: Bootloader::new(engine, actor_system.clone()),
			subsystems: subsystem_manager,
			health_monitor,
			shared_runtime,
			actor_system,
			running: false,
			migrations,
		}
	}

	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	pub fn auth_service(&self) -> &AuthService {
		&self.auth_service
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

		self.bootloader.load()?;

		// Apply pending migrations if any were registered
		if !self.migrations.is_empty() {
			self.apply_migrations()?;
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

		// Stop all subsystems (now synchronously waits for each to finish)
		self.subsystems.stop_all()?;

		self.actor_system.shutdown();
		let _ = self.actor_system.join();

		self.engine.shutdown();

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

	/// Apply registered migrations: CREATE MIGRATION for any new ones, then MIGRATE to apply pending.
	fn apply_migrations(&self) -> Result<()> {
		debug!("Applying {} registered migrations", self.migrations.len());

		for migration in &self.migrations {
			// Build CREATE MIGRATION statement
			let mut rql = format!("CREATE MIGRATION '{}' {{", migration.name);
			rql.push_str(&migration.body);
			rql.push('}');

			if let Some(ref rollback) = migration.rollback_body {
				rql.push_str(" ROLLBACK {");
				rql.push_str(rollback);
				rql.push('}');
			}

			rql.push(';');

			// Try to create — ignore "already exists" errors
			match self.admin_as_root(&rql, Params::None) {
				Ok(_) => {
					debug!("Registered migration '{}'", migration.name);
				}
				Err(e) => {
					let msg = format!("{}", e);
					if msg.contains("already exists") {
						debug!("Migration '{}' already registered, skipping", migration.name);
					} else {
						return Err(e);
					}
				}
			}
		}

		// Apply all pending migrations
		debug!("Running MIGRATE to apply pending migrations");
		let result = self.admin_as_root("MIGRATE;", Params::None)?;
		if let Some(frame) = result.first()
			&& let Ok(Some(count)) = frame.get::<u32>("migrations_applied", 0)
		{
			debug!("Applied {} pending migrations", count);
		}

		Ok(())
	}

	pub fn get_subsystem_names(&self) -> Vec<String> {
		self.subsystems.get_subsystem_names()
	}

	pub fn subsystem<S: 'static>(&self) -> Option<&S> {
		self.subsystems.get::<S>()
	}

	/// Execute an admin (DDL + DML + Query) operation as root user.
	pub fn admin_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>> {
		let r = self.engine.admin_as(IdentityId::root(), rql, params.into());
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
	}

	/// Execute a transactional command (DML + Query) as root user.
	pub fn command_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>> {
		let r = self.engine.command_as(IdentityId::root(), rql, params.into());
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
	}

	/// Execute a read-only query as root user.
	pub fn query_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>> {
		let r = self.engine.query_as(IdentityId::root(), rql, params.into());
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
	}

	/// Execute an admin (DDL + DML + Query) operation as a specific identity.
	pub fn admin_as(&self, identity: IdentityId, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>> {
		let r = self.engine.admin_as(identity, rql, params.into());
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
	}

	/// Execute a transactional command (DML + Query) as a specific identity.
	pub fn command_as(&self, identity: IdentityId, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>> {
		let r = self.engine.command_as(identity, rql, params.into());
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
	}

	/// Execute a read-only query as a specific identity.
	pub fn query_as(&self, identity: IdentityId, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>> {
		let r = self.engine.query_as(identity, rql, params.into());
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
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

		extern "C" fn handle_signal(_sig: c_int) {
			// SAFETY: Only async-signal-safe operations are allowed here.
			// We only use atomic operations, which are signal-safe.
			RUNNING.store(false, Ordering::SeqCst);
			SIGNAL_RECEIVED.store(true, Ordering::SeqCst);
		}

		unsafe {
			signal(SIGINT, handle_signal as sighandler_t);
			signal(SIGTERM, handle_signal as sighandler_t);
			signal(SIGQUIT, handle_signal as sighandler_t);
			signal(SIGHUP, handle_signal as sighandler_t);
		}

		debug!("Waiting for termination signal...");
		while RUNNING.load(Ordering::SeqCst) {
			sleep(Duration::from_millis(100));

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
		// Always break the Engine ↔ IoC cycle, even if we were never started.
		// Without this, the engine→IoC→engine reference cycle keeps SharedRuntime
		// (and the tokio runtime's FDs) alive indefinitely.
		self.engine.shutdown();
	}
}

impl Database {
	/// Create a session for the given identity.
	pub fn session(&self, identity: IdentityId) -> Session {
		Session::trusted(self.engine.clone(), identity)
	}

	/// Create a session as the root user.
	pub fn root_session(&self) -> Session {
		Session::trusted(self.engine.clone(), IdentityId::root())
	}
}
