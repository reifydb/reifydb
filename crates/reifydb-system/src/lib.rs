// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! System-level coordination for ReifyDB Engine and Subsystems
//!
//! This crate provides a comprehensive architecture for managing the ReifyDB Engine
//! along with multiple subsystems in a coordinated fashion. It handles lifecycle
//! management, health monitoring, and graceful shutdown without using async/await.

mod builder;
mod context;
mod health;
mod manager;
mod subsystem;
mod system;

pub use builder::ReifySystemBuilder;
#[cfg(feature = "async")]
pub use context::TokioRuntimeProvider;
pub use context::{
    AsyncContext, CustomContext, RuntimeProvider, SyncContext, SystemContext, TokioContext,
};
pub use health::{HealthMonitor, HealthStatus};

pub use manager::SubsystemManager;
#[cfg(feature = "sub_flow")]
pub use subsystem::FlowSubsystemAdapter;
#[cfg(feature = "sub_grpc")]
pub use subsystem::GrpcSubsystemAdapter;
pub use subsystem::Subsystem;
#[cfg(feature = "sub_ws")]
pub use subsystem::WsSubsystemAdapter;
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
