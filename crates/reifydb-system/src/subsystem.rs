// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::health::HealthStatus;
use reifydb_core::Result;

/// Uniform interface that all subsystems must implement
///
/// This trait provides a consistent lifecycle and monitoring interface
/// for all subsystems managed by the ReifySystem.
pub trait Subsystem: Send + Sync {
    /// Get the unique name of this subsystem
    fn name(&self) -> &str;

    /// Start the subsystem
    ///
    /// This method should initialize the subsystem and start any background
    /// threads or processes. It should be idempotent - calling start() on
    /// an already running subsystem should succeed without side effects.
    fn start(&mut self) -> Result<()>;

    /// Stop the subsystem
    ///
    /// This method should gracefully shut down the subsystem and clean up
    /// any resources. It should be idempotent - calling stop() on an
    /// already stopped subsystem should succeed without side effects.
    fn stop(&mut self) -> Result<()>;

    /// Check if the subsystem is currently running
    fn is_running(&self) -> bool;

    /// Get the current health status of the subsystem
    ///
    /// This should provide information about the subsystem's operational
    /// status and any errors or warnings.
    fn health_status(&self) -> HealthStatus;
}