// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod api;
mod boot;
mod builder;
mod context;
mod database;
pub mod event;
mod health;
mod session;
pub mod subsystem;

use std::time::Duration;

pub use api::*;
#[cfg(feature = "sub_server")]
pub use builder::ServerBuilder;
pub use builder::{DatabaseBuilder, EmbeddedBuilder, WithSubsystem};
pub use context::{RuntimeProvider, SyncContext, SystemContext};
pub use database::{Database, DatabaseConfig};
pub use event::{OnCreateContext, WithEventBus};
pub use health::HealthMonitor;
pub use reifydb_auth as auth;
pub use reifydb_cdc as cdc;
// subsystems
pub use reifydb_core as core;
pub use reifydb_core::{
	Error, Result,
	event::EventBus,
	interface::{Identity, MultiVersionTransaction, Params, SingleVersionTransaction},
	log, log_critical, log_debug, log_error, log_info, log_timed_critical, log_timed_debug, log_timed_error,
	log_timed_info, log_timed_trace, log_timed_warn, log_trace, log_warn,
};
pub use reifydb_engine as engine;
pub use reifydb_network as network;
pub use reifydb_rql as rql;
pub use reifydb_store_transaction as storage;
pub use reifydb_store_transaction::backend::{
	memory::MemoryBackend,
	sqlite::{SqliteBackend, SqliteConfig},
};
#[cfg(feature = "sub_admin")]
pub use reifydb_sub_admin as sub_admin;
pub use reifydb_sub_api as sub;
#[cfg(feature = "sub_flow")]
pub use reifydb_sub_flow as sub_flow;
#[cfg(feature = "sub_logging")]
pub use reifydb_sub_logging as sub_logging;
#[cfg(feature = "sub_server")]
pub use reifydb_sub_server as sub_server;
pub use reifydb_sub_worker as sub_worker;
pub use reifydb_transaction as transaction;
pub use reifydb_transaction::{
	multi::transaction::{optimistic::TransactionOptimistic, serializable::TransactionSerializable},
	single::TransactionSvl,
};
pub use reifydb_type as r#type;
pub use session::{CommandSession, QuerySession, Session};

/// Default configuration values
pub mod defaults {
	use super::Duration;

	/// Default graceful shutdown timeout (30 seconds)
	pub const GRACEFUL_SHSVTDOWN_TIMEOSVT: Duration = Duration::from_secs(30);

	/// Default health check interval (5 seconds)
	pub const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

	/// Default maximum startup time (60 seconds)
	pub const MAX_STARTUP_TIME: Duration = Duration::from_secs(60);
}
