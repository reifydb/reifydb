// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::health::{HealthMonitor, HealthStatus};
use crate::manager::SubsystemManager;
#[cfg(feature = "async")]
use crate::session::SessionAsync;
use crate::session::{
    CommandSession, IntoCommandSession, IntoQuerySession, QuerySession, Session, SessionSync,
};
use reifydb_core::Result;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "sub_flow")]
use crate::FlowSubsystemAdapter;
#[cfg(feature = "sub_grpc")]
use crate::GrpcSubsystemAdapter;
#[cfg(feature = "sub_ws")]
use crate::WsSubsystemAdapter;

/// System configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Maximum time to wait for graceful shutdown
    pub graceful_shutdown_timeout: Duration,
    /// Interval for health checks
    pub health_check_interval: Duration,
    /// Maximum time allowed for system startup
    pub max_startup_time: Duration,
}

impl DatabaseConfig {
    /// Create a new system configuration with default values
    pub fn new() -> Self {
        Self {
            graceful_shutdown_timeout: crate::defaults::GRACEFUL_SHUTDOWN_TIMEOUT,
            health_check_interval: crate::defaults::HEALTH_CHECK_INTERVAL,
            max_startup_time: crate::defaults::MAX_STARTUP_TIME,
        }
    }

    /// Set the graceful shutdown timeout
    pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.graceful_shutdown_timeout = timeout;
        self
    }

    /// Set the health check interval
    pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Set the maximum startup time
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

/// Main system coordinator that manages Engine and all subsystems
///
/// Database provides a unified interface for managing the entire ReifyDB
/// system lifecycle, including the engine and all associated subsystems.
pub struct Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// The ReifyDB engine
    engine: Engine<VT, UT>,
    /// Subsystem manager
    subsystem_manager: SubsystemManager,
    /// System configuration
    config: DatabaseConfig,
    /// Health monitor
    health_monitor: Arc<HealthMonitor>,
    /// System running state
    running: bool,
}

impl<VT, UT> Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[cfg(feature = "sub_flow")]
    pub fn subsystem_flow(&self) -> Option<&FlowSubsystemAdapter<VT, UT>> {
        self.subsystem::<FlowSubsystemAdapter<VT, UT>>()
    }

    #[cfg(feature = "sub_grpc")]
    pub fn subsystem_grpc(&self) -> Option<&GrpcSubsystemAdapter<VT, UT>> {
        self.subsystem::<GrpcSubsystemAdapter<VT, UT>>()
    }

    #[cfg(feature = "sub_ws")]
    pub fn subsystem_ws(&self) -> Option<&WsSubsystemAdapter<VT, UT>> {
        self.subsystem::<WsSubsystemAdapter<VT, UT>>()
    }
}

impl<VT, UT> Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Create a new Database (typically use DatabaseBuilder instead)
    pub(crate) fn new(
        engine: Engine<VT, UT>,
        subsystem_manager: SubsystemManager,
        config: DatabaseConfig,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self { engine, subsystem_manager, config, health_monitor, running: false }
    }

    /// Get a reference to the engine
    pub fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }

    /// Get the system configuration
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Check if the system is running
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Get the number of managed subsystems
    pub fn subsystem_count(&self) -> usize {
        self.subsystem_manager.subsystem_count()
    }

    /// Start the entire system
    ///
    /// This starts the engine (if needed) and all subsystems in a coordinated manner.
    /// If any component fails to start, the entire startup process is aborted and
    /// all successfully started components are stopped.
    pub fn start(&mut self) -> Result<()> {
        if self.running {
            return Ok(()); // Already running
        }

        println!("[Database] Starting system with {} subsystems", self.subsystem_count());

        // Initialize engine health monitoring
        self.health_monitor.update_component_health(
            "engine".to_string(),
            HealthStatus::Healthy, // Engine is always healthy if constructed
            true,
        );

        // Start all subsystems
        match self.subsystem_manager.start_all(self.config.max_startup_time) {
            Ok(()) => {
                self.running = true;
                println!("[Database] System started successfully");
                self.update_health_monitoring();
                Ok(())
            }
            Err(e) => {
                eprintln!("[Database] System startup failed: {}", e);
                // Update system health to reflect failure
                self.health_monitor.update_component_health(
                    "system".to_string(),
                    HealthStatus::Failed { message: format!("Startup failed: {}", e) },
                    false,
                );
                Err(e)
            }
        }
    }

    /// Stop the entire system gracefully
    ///
    /// This stops all subsystems in reverse order and performs cleanup.
    /// The system attempts to shut down gracefully within the configured timeout.
    pub fn stop(&mut self) -> Result<()> {
        if !self.running {
            return Ok(()); // Already stopped
        }

        println!("[Database] Stopping system gracefully");

        // Stop all subsystems
        let result = self.subsystem_manager.stop_all(self.config.graceful_shutdown_timeout);

        // Update engine health monitoring (engine is stopped when system stops)
        self.health_monitor.update_component_health(
            "engine".to_string(),
            HealthStatus::Healthy,
            false,
        );

        self.running = false;

        match result {
            Ok(()) => {
                println!("[Database] System stopped successfully");
                self.health_monitor.update_component_health(
                    "system".to_string(),
                    HealthStatus::Healthy,
                    false,
                );
                Ok(())
            }
            Err(e) => {
                eprintln!("[Database] System shutdown completed with errors: {}", e);
                self.health_monitor.update_component_health(
                    "system".to_string(),
                    HealthStatus::Warning {
                        message: format!("Shutdown completed with errors: {}", e),
                    },
                    false,
                );
                Err(e)
            }
        }
    }

    /// Get the current health status of the entire system
    pub fn health_status(&self) -> HealthStatus {
        self.health_monitor.get_system_health()
    }

    /// Get health status of all components
    pub fn get_all_component_health(
        &self,
    ) -> std::collections::HashMap<String, crate::health::ComponentHealth> {
        self.health_monitor.get_all_health()
    }

    /// Update health monitoring for all components
    pub fn update_health_monitoring(&mut self) {
        // Update subsystem health
        self.subsystem_manager.update_health_monitoring();

        // Update system health
        let system_health = if self.running {
            self.health_monitor.get_system_health()
        } else {
            HealthStatus::Healthy
        };

        self.health_monitor.update_component_health(
            "system".to_string(),
            system_health,
            self.running,
        );
    }

    /// Get the names of all managed subsystems
    pub fn get_subsystem_names(&self) -> Vec<String> {
        self.subsystem_manager.get_subsystem_names()
    }

    /// Check for components with stale health information
    pub fn get_stale_components(&self) -> Vec<String> {
        self.health_monitor.get_stale_components(self.config.health_check_interval * 2)
    }

    /// Get the gRPC socket address if the gRPC subsystem is running
    #[cfg(feature = "sub_grpc")]
    pub fn grpc_socket_addr(&self) -> Option<std::net::SocketAddr> {
        if let Some(subsystem) = self.subsystem_grpc() {
            return subsystem.socket_addr();
        }
        None
    }

    /// Get the WebSocket socket address if the WebSocket subsystem is running
    #[cfg(feature = "sub_ws")]
    pub fn ws_socket_addr(&self) -> Option<std::net::SocketAddr> {
        if let Some(subsystem) = self.subsystem_ws() {
            return subsystem.socket_addr();
        }
        None
    }

    /// Get a reference to a subsystem of a specific type
    ///
    /// This method attempts to downcast each managed subsystem to the requested type T.
    /// Returns the first subsystem that matches the type, or None if no subsystem of that type is found.
    ///
    pub fn subsystem<T: 'static>(&self) -> Option<&T> {
        for subsystem in &self.subsystem_manager.subsystems {
            if let Some(typed_subsystem) = subsystem.as_any().downcast_ref::<T>() {
                return Some(typed_subsystem);
            }
        }
        None
    }
}

impl<VT, UT> Drop for Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn drop(&mut self) {
        if self.running {
            println!("[Database] System being dropped while running, attempting graceful shutdown");
            let _ = self.stop();
        }
    }
}

// Session trait implementations for Database
impl<VT, UT> Session<VT, UT> for Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn command_session(
        &self,
        session: impl IntoCommandSession<VT, UT>,
    ) -> Result<CommandSession<VT, UT>> {
        session.into_command_session(self.engine.clone())
    }

    fn query_session(
        &self,
        session: impl IntoQuerySession<VT, UT>,
    ) -> Result<QuerySession<VT, UT>> {
        session.into_query_session(self.engine.clone())
    }
}

impl<VT, UT> SessionSync<VT, UT> for Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
}

#[cfg(feature = "async")]
impl<VT, UT> SessionAsync<VT, UT> for Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
}
