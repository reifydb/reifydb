// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::database::{Database, DatabaseConfig};
use crate::health::HealthMonitor;
use crate::{Subsystem, Subsystems};
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;
use std::sync::Arc;
use std::time::Duration;

pub struct DatabaseBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
    config: DatabaseConfig,
    subsystems: Vec<Box<dyn Subsystem>>,
}

impl<VT, UT> DatabaseBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[allow(unused_mut)]
    pub fn new(engine: Engine<VT, UT>) -> Self {
        let mut result = Self {
            engine: engine.clone(),
            config: DatabaseConfig::default(),
            subsystems: Vec::new(),
        };

        #[cfg(feature = "sub_flow")]
        {
            use std::time::Duration;
            let flow_subsystem = crate::FlowSubsystem::new(engine, Duration::from_millis(100));
            result = result.add_subsystem(flow_subsystem);
        }

        result
    }

    pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_graceful_shutdown_timeout(timeout);
        self
    }

    pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
        self.config = self.config.with_health_check_interval(interval);
        self
    }

    pub fn with_max_startup_time(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_max_startup_time(timeout);
        self
    }

    pub fn with_config(mut self, config: DatabaseConfig) -> Self {
        self.config = config;
        self
    }

    pub fn add_subsystem(mut self, subsystem: impl Subsystem + 'static) -> Self {
        self.subsystems.push(Box::new(subsystem));
        self
    }

    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    pub fn subsystem_count(&self) -> usize {
        self.subsystems.len()
    }

    pub fn build(self) -> Database<VT, UT> {
        let health_monitor = Arc::new(HealthMonitor::new());

        let mut subsystem_manager = Subsystems::new(Arc::clone(&health_monitor));
        for subsystem in self.subsystems {
            subsystem_manager.add_subsystem(subsystem);
        }

        Database::new(self.engine, subsystem_manager, self.config, health_monitor)
    }
}

impl<VT, UT> DatabaseBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn development_config(self) -> Self {
        self.with_graceful_shutdown_timeout(Duration::from_secs(10))
            .with_health_check_interval(Duration::from_secs(2))
            .with_max_startup_time(Duration::from_secs(30))
    }

    pub fn production_config(self) -> Self {
        self.with_graceful_shutdown_timeout(Duration::from_secs(60))
            .with_health_check_interval(Duration::from_secs(10))
            .with_max_startup_time(Duration::from_secs(120))
    }

    pub fn testing_config(self) -> Self {
        self.with_graceful_shutdown_timeout(Duration::from_secs(5))
            .with_health_check_interval(Duration::from_secs(1))
            .with_max_startup_time(Duration::from_secs(10))
    }
}
