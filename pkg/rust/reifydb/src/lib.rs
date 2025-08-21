// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod api;
mod boot;
mod builder;
mod context;
mod database;
mod health;
mod hook;
mod presets;
mod session;
pub mod subsystem;

use std::time::Duration;

pub use api::*;
pub use builder::*;
#[cfg(feature = "async")]
pub use context::TokioRuntimeProvider;
pub use context::{AsyncContext, RuntimeProvider, SystemContext, TokioContext};
pub use database::{Database, DatabaseConfig};
pub use health::HealthMonitor;
pub use hook::{OnCreateContext, WithHooks};
pub use presets::*;
pub use reifydb_auth as auth;
pub use reifydb_core as core;
pub use reifydb_core::{
    hook::Hooks, interface::{
        StandardTransaction, UnversionedTransaction, VersionedStorage,
        VersionedTransaction,
    },
    Error,
    Result,
};
pub use reifydb_engine as engine;
#[cfg(feature = "sub_flow")]
pub use reifydb_flow as flow;
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub use reifydb_network as network;
pub use reifydb_rql as rql;
pub use reifydb_storage as storage;
pub use reifydb_storage::{
    lmdb::Lmdb,
    memory::Memory,
    sqlite::{Sqlite, SqliteConfig},
};
pub use reifydb_transaction as transaction;
pub use reifydb_transaction::{
    mvcc::transaction::{
        optimistic::Optimistic, serializable::Serializable,
    },
    svl::SingleVersionLock,
};
pub use session::{
    CommandSession, CommandSessionSync, QuerySession, QuerySessionSync,
    Session, SessionSync,
};

pub use reifydb_core::interface::{Identity, Params};
pub use reifydb_core::{
    log, log_critical, log_debug, log_error, log_info, log_timed_critical,
    log_timed_debug, log_timed_error, log_timed_info, log_timed_trace,
    log_timed_warn, log_trace, log_warn,
};

#[cfg(feature = "async")]
pub use session::{CommandSessionAsync, QuerySessionAsync, SessionAsync};

// subsystems
#[cfg(feature = "sub_logging")]
pub use reifydb_sub_logging::{FormatStyle, LoggingBuilder};

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
