// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::database::{Database, DatabaseConfig};
use crate::health::HealthMonitor;
use crate::manager::SubsystemManager;
use crate::subsystem::Subsystem;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;
use std::sync::Arc;
use std::time::Duration;

/// Builder for configuring and constructing a Database
///
/// The DatabaseBuilder provides a fluent interface for configuring
/// the ReifyDB system before starting it up. This includes setting
/// timeouts, adding subsystems, and configuring health monitoring.
///
/// # Example
/// ```rust,ignore
/// use reifydb_system::DatabaseBuilder;
/// use std::time::Duration;
///
/// let mut system = DatabaseBuilder::new(engine)
///     .with_graceful_shutdown_timeout(Duration::from_secs(30))
///     .with_health_check_interval(Duration::from_secs(5))
///     .add_subsystem(Box::new(my_subsystem))
///     .build();
///
/// system.start()?;
/// // ... system is running
/// system.stop()?;
/// ```
pub struct DatabaseBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// The ReifyDB engine
    engine: Engine<VT, UT>,
    /// System configuration being built
    config: DatabaseConfig,
    /// Subsystems to be managed
    subsystems: Vec<Box<dyn Subsystem>>,
}

impl<VT, UT> DatabaseBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Create a new builder with the given engine
    pub fn new(engine: Engine<VT, UT>) -> Self {
        Self { engine, config: DatabaseConfig::default(), subsystems: Vec::new() }
    }

    /// Set the graceful shutdown timeout
    ///
    /// This is the maximum time the system will wait for all subsystems
    /// to stop gracefully before forcefully terminating them.
    pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_graceful_shutdown_timeout(timeout);
        self
    }

    /// Set the health check interval
    ///
    /// This determines how frequently the system checks and updates
    /// the health status of all components.
    pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
        self.config = self.config.with_health_check_interval(interval);
        self
    }

    /// Set the maximum startup time
    ///
    /// This is the maximum time allowed for the entire system startup
    /// process. If startup takes longer, it will be aborted and rolled back.
    pub fn with_max_startup_time(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_max_startup_time(timeout);
        self
    }

    /// Set a complete system configuration
    ///
    /// This replaces the current configuration entirely. Use the individual
    /// `with_*` methods if you want to modify specific settings.
    pub fn with_config(mut self, config: DatabaseConfig) -> Self {
        self.config = config;
        self
    }

    /// Add a subsystem to be managed by the system
    ///
    /// Subsystems will be started in the order they are added and stopped
    /// in reverse order. Each subsystem must implement the Subsystem trait.
    pub fn add_subsystem(mut self, subsystem: Box<dyn Subsystem>) -> Self {
        self.subsystems.push(subsystem);
        self
    }

    /// Add multiple subsystems at once
    pub fn add_subsystems(mut self, mut subsystems: Vec<Box<dyn Subsystem>>) -> Self {
        self.subsystems.append(&mut subsystems);
        self
    }

    /// Get the current configuration (for inspection)
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Get the number of subsystems that will be managed
    pub fn subsystem_count(&self) -> usize {
        self.subsystems.len()
    }

    /// Build the Database
    ///
    /// This consumes the builder and creates a fully configured Database
    /// that is ready to be started. The system is not automatically started
    /// by this method - you must call `start()` explicitly.
    pub fn build(self) -> Database<VT, UT> {
        // Create shared health monitor
        let health_monitor = Arc::new(HealthMonitor::new());

        // Create subsystem manager and add all subsystems
        let mut subsystem_manager = SubsystemManager::new(Arc::clone(&health_monitor));
        for subsystem in self.subsystems {
            subsystem_manager.add_subsystem(subsystem);
        }

        // Create the system
        Database::new(self.engine, subsystem_manager, self.config, health_monitor)
    }
}

/// Convenience methods for common builder patterns
impl<VT, UT> DatabaseBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Configure for development (shorter timeouts, more frequent health checks)
    pub fn development_config(self) -> Self {
        self.with_graceful_shutdown_timeout(Duration::from_secs(10))
            .with_health_check_interval(Duration::from_secs(2))
            .with_max_startup_time(Duration::from_secs(30))
    }

    /// Configure for production (longer timeouts, less frequent health checks)
    pub fn production_config(self) -> Self {
        self.with_graceful_shutdown_timeout(Duration::from_secs(60))
            .with_health_check_interval(Duration::from_secs(10))
            .with_max_startup_time(Duration::from_secs(120))
    }

    /// Configure for testing (very short timeouts)
    pub fn testing_config(self) -> Self {
        self.with_graceful_shutdown_timeout(Duration::from_secs(5))
            .with_health_check_interval(Duration::from_secs(1))
            .with_max_startup_time(Duration::from_secs(10))
    }
}
