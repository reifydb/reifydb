// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Subsystem trait and implementations
//!
//! This module defines the core Subsystem trait and provides concrete implementations
//! and adapters for various subsystem types like Flow, gRPC, and WebSocket servers.

use crate::health::HealthStatus;
use reifydb_core::Result;

#[cfg(feature = "sub_flow")]
mod flow;
#[cfg(feature = "sub_grpc")]
pub(crate) mod grpc;
#[cfg(feature = "sub_ws")]
pub(crate) mod websocket;

#[cfg(feature = "sub_flow")]
pub use flow::FlowSubsystemAdapter;
#[cfg(feature = "sub_grpc")]
pub use grpc::GrpcSubsystemAdapter;
#[cfg(feature = "sub_ws")]
pub use websocket::WsSubsystemAdapter;

/// Uniform interface that all subsystems must implement
///
/// This trait provides a consistent lifecycle and monitoring interface
/// for all subsystems managed by the Database.
pub trait Subsystem: Send + Sync + std::any::Any {
    /// Get the unique name of this subsystem
    fn name(&self) -> &'static str;

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

    /// Get a reference to self as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}
