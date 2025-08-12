// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod api;
mod builder;
mod context;
mod database;
mod health;
mod hook;
mod presets;
mod session;
mod subsystem;

pub use api::*;
pub use reifydb_auth as auth;
pub use reifydb_core as core;
pub use reifydb_core::{Error, Result};
pub use reifydb_engine as engine;
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub use reifydb_network as network;
pub use reifydb_rql as rql;
pub use reifydb_storage as storage;
pub use reifydb_transaction as transaction;

pub use builder::*;
#[cfg(feature = "async")]
pub use context::TokioRuntimeProvider;
pub use context::{AsyncContext, RuntimeProvider, SystemContext, TokioContext};
pub use health::{HealthMonitor, HealthStatus};
pub use hook::{OnCreateContext, WithHooks};

pub use database::{Database, DatabaseConfig};
#[cfg(feature = "async")]
pub use session::SessionAsync;
pub use session::{CommandSession, QuerySession, Session, SessionSync};
#[cfg(feature = "sub_flow")]
pub use subsystem::FlowSubsystem;
#[cfg(feature = "sub_grpc")]
pub use subsystem::GrpcSubsystem;
pub use subsystem::Subsystem;
pub use subsystem::Subsystems;
#[cfg(feature = "sub_ws")]
pub use subsystem::WsSubsystem;

use std::time::Duration;

pub use presets::*;
pub use reifydb_core::hook::Hooks;
pub use reifydb_core::interface::{StandardTransaction, UnversionedTransaction, VersionedStorage, VersionedTransaction};
pub use reifydb_storage::lmdb::Lmdb;
pub use reifydb_storage::memory::Memory;
pub use reifydb_storage::sqlite::{Sqlite, SqliteConfig};
pub use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
pub use reifydb_transaction::mvcc::transaction::serializable::Serializable;
pub use reifydb_transaction::svl::SingleVersionLock;

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

