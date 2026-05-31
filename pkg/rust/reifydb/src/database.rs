// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
use reifydb_catalog::catalog::Catalog;
use reifydb_cdc::storage::CdcStore;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	RuntimeHandle,
	actor::{mailbox::ActorRef, system::ActorSpawner},
	context::clock::Clock,
	pool::Pools,
	shutdown::Shutdown,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
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
use reifydb_value::{
	Result,
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};
use tracing::{debug, instrument, warn};

#[cfg(feature = "sub_raft")]
use crate::raft::RaftSubsystem;
use crate::{
	health::{ComponentHealth, HealthMonitor},
	session::Session,
	subsystem::Subsystems,
	watermarks::Watermarks,
};

const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Database {
	engine: StandardEngine,
	auth_service: AuthService,
	subsystems: Subsystems,
	health_monitor: Arc<HealthMonitor>,
	spawner: ActorSpawner,
	clock: Clock,
	runtime: RuntimeHandle,
	running: bool,
	fast_shutdown: bool,
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

	#[cfg(feature = "sub_raft")]
	pub fn sub_raft(&self) -> Option<&RaftSubsystem> {
		self.subsystem::<RaftSubsystem>()
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
		spawner: ActorSpawner,
		clock: Clock,
		runtime: RuntimeHandle,
	) -> Self {
		Self {
			engine,
			auth_service,
			subsystems: subsystem_manager,
			health_monitor,
			spawner,
			clock,
			runtime,
			running: true,
			fast_shutdown: false,
		}
	}

	pub(crate) fn fast_shutdown_on_drop(mut self, v: bool) -> Self {
		self.fast_shutdown = v;
		self
	}

	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	pub fn catalog(&self) -> Catalog {
		self.engine.catalog()
	}

	/// Borrowed view over the database's progress watermarks. Use to ask
	/// "is the CDC producer caught up?", "what's the last applied replica
	/// version?", etc. via the chained accessors.
	pub fn watermarks(&self) -> Watermarks<'_> {
		Watermarks::new(self)
	}

	/// Resolve an actor handle by message type. Returns `None` if no actor
	/// for `M` was registered during engine construction.
	pub fn actor<M: 'static>(&self) -> Option<ActorRef<M>>
	where
		ActorRef<M>: Send + Sync,
	{
		self.engine.actor::<M>()
	}

	pub fn auth_service(&self) -> &AuthService {
		&self.auth_service
	}

	pub fn clock(&self) -> &Clock {
		&self.clock
	}

	pub fn pools(&self) -> Pools {
		self.spawner.pools()
	}

	pub fn runtime(&self) -> &RuntimeHandle {
		&self.runtime
	}

	pub fn is_running(&self) -> bool {
		self.running
	}

	pub fn subsystem_count(&self) -> usize {
		self.subsystems.subsystem_count()
	}

	#[instrument(name = "api::database::stop", level = "debug", skip(self))]
	pub fn stop(&mut self) -> Result<()> {
		self.shutdown_internal(!self.fast_shutdown)
	}

	#[instrument(name = "api::database::stop_fast", level = "debug", skip(self))]
	pub fn stop_fast(&mut self) -> Result<()> {
		self.shutdown_internal(false)
	}

	fn shutdown_internal(&mut self, drain: bool) -> Result<()> {
		if !self.running {
			return Ok(()); // Already stopped
		}

		debug!("Stopping system");

		self.engine.set_shutting_down();

		if drain {
			self.drain_cdc_consumers(SHUTDOWN_DRAIN_TIMEOUT);

			if let Some(multi_store) = self.engine.ioc().try_resolve::<MultiStore>() {
				multi_store.flush_all_blocking();
			}

			if let Some(single_store) = self.engine.ioc().try_resolve::<SingleStore>() {
				single_store.flush_pending_blocking();
			}
		}

		self.subsystems.shutdown_all();

		if let Some(multi_store) = self.engine.ioc().try_resolve::<MultiStore>() {
			multi_store.shutdown();
		}
		if let Some(single_store) = self.engine.ioc().try_resolve::<SingleStore>() {
			single_store.shutdown();
		}
		if let Some(cdc_store) = self.engine.ioc().try_resolve::<CdcStore>() {
			cdc_store.shutdown();
		}

		self.engine.shutdown();

		self.running = false;
		debug!("System stopped successfully");
		self.health_monitor.update_component_health("system".to_string(), HealthStatus::Healthy, false);
		Ok(())
	}

	#[cfg(all(feature = "sub_flow", not(reifydb_single_threaded)))]
	fn drain_cdc_consumers(&self, timeout: Duration) {
		if self.sub_flow().is_none() {
			return;
		}

		const PLATEAU_ROUNDS: u32 = 6;

		let deadline = self.clock.instant() + timeout;
		let mut last_producer = u64::MAX;
		let mut last_consumer = u64::MAX;
		let mut caught_up = 0u32;
		let mut plateaued = 0u32;
		loop {
			let producer = self.engine.cdc_producer_watermark().0;
			let consumer = self.engine.consumer_watermark().0;

			if consumer >= producer && producer == last_producer {
				caught_up += 1;
				if caught_up >= 2 {
					return;
				}
			} else {
				caught_up = 0;
			}

			if producer == last_producer && consumer == last_consumer {
				plateaued += 1;
				if plateaued >= PLATEAU_ROUNDS {
					return;
				}
			} else {
				plateaued = 0;
			}

			last_producer = producer;
			last_consumer = consumer;

			if self.clock.instant() >= deadline {
				warn!(
					producer,
					consumer, "shutdown drain timed out; flushing already-committed data anyway"
				);
				return;
			}

			self.engine.notify_cdc_consumers();
			sleep(Duration::from_millis(50));
		}
	}

	#[cfg(not(all(feature = "sub_flow", not(reifydb_single_threaded))))]
	fn drain_cdc_consumers(&self, _timeout: Duration) {}

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
		debug!("Database running, waiting for termination signal...");

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
			let _ = self.shutdown_internal(!self.fast_shutdown);
		}
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
