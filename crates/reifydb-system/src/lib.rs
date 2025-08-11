// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! System-level coordination for ReifyDB Engine and Subsystems
//!
//! This crate provides a comprehensive architecture for managing the ReifyDB Engine
//! along with multiple subsystems in a coordinated fashion. It handles lifecycle
//! management, health monitoring, and graceful shutdown without using async/await.

pub mod adapters;
pub mod builder;
pub mod context;
pub mod health;
pub mod manager;
pub mod subsystem;
pub mod system;

pub use adapters::FlowSubsystemAdapter;
#[cfg(any(feature = "server", feature = "grpc"))]
pub use adapters::GrpcSubsystemAdapter;
#[cfg(any(feature = "server", feature = "websocket"))]
pub use adapters::WsSubsystemAdapter;
pub use builder::ReifySystemBuilder;
pub use context::{
    AsyncContext, CustomContext, RuntimeProvider, SyncContext, SystemContext, 
    TokioContext, TokioRuntimeProvider
};
pub use health::{HealthMonitor, HealthStatus};
pub use manager::SubsystemManager;
pub use subsystem::Subsystem;
pub use system::{ReifySystem, SystemConfig};

use std::time::Duration;

/// Default configuration values
pub mod defaults {
    use super::Duration;

    /// Default graceful shutdown timeout (30 seconds)
    pub const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

    /// Default health check interval (5 seconds)  
    pub const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

    /// Default maximum startup time (60 seconds)
    pub const MAX_STARTUP_TIME: Duration = Duration::from_secs(60);
}