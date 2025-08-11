// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! System-level coordination for ReifyDB Engine and Subsystems
//!
//! This crate provides a comprehensive architecture for managing the ReifyDB Engine
//! along with multiple subsystems in a coordinated fashion. It handles lifecycle
//! management, health monitoring, and graceful shutdown without using async/await.

mod builder;
mod context;
mod database;
mod health;
mod hook;
mod manager;
mod session;
mod subsystem;
mod variant;

pub use reifydb_auth as auth;
pub use reifydb_core as core;
pub use reifydb_core::{Error, Result};
pub use reifydb_engine as engine;
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub use reifydb_network as network;
pub use reifydb_rql as rql;
pub use reifydb_storage as storage;
pub use reifydb_transaction as transaction;

pub use builder::DatabaseBuilder;
#[cfg(feature = "async")]
pub use context::TokioRuntimeProvider;
pub use context::{
    AsyncContext, CustomContext, RuntimeProvider, SyncContext, SystemContext, TokioContext,
};
pub use health::{HealthMonitor, HealthStatus};
pub use hook::{OnCreateContext, WithHooks};

pub use database::{Database, DatabaseConfig};
pub use manager::SubsystemManager;
#[cfg(feature = "async")]
pub use session::SessionAsync;
pub use session::{CommandSession, QuerySession, Session, SessionSync};
#[cfg(feature = "sub_flow")]
pub use subsystem::FlowSubsystemAdapter;
#[cfg(feature = "sub_grpc")]
pub use subsystem::GrpcSubsystemAdapter;
pub use subsystem::Subsystem;
#[cfg(feature = "sub_ws")]
pub use subsystem::WsSubsystemAdapter;

#[cfg(feature = "async")]
pub use variant::AsyncBuilder;
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub use variant::ServerBuilder;
pub use variant::SyncBuilder;

use std::path::Path;
use std::time::Duration;

pub use reifydb_core::hook::Hooks;
pub use reifydb_core::interface::{UnversionedTransaction, VersionedStorage, VersionedTransaction};
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

/// Convenience function to create an optimistic transaction layer
pub fn optimistic<VS, UT>(input: (VS, UT, Hooks)) -> (Optimistic<VS, UT>, UT, Hooks)
where
    VS: VersionedStorage,
    UT: UnversionedTransaction,
{
    (Optimistic::new(input.0, input.1.clone(), input.2.clone()), input.1, input.2)
}

/// Convenience function to create a serializable transaction layer
pub fn serializable<VS, UT>(input: (VS, UT, Hooks)) -> (Serializable<VS, UT>, UT, Hooks)
where
    VS: VersionedStorage,
    UT: UnversionedTransaction,
{
    (Serializable::new(input.0, input.1.clone(), input.2.clone()), input.1, input.2)
}

/// Convenience function to create in-memory storage
pub fn memory() -> (Memory, SingleVersionLock<Memory>, Hooks) {
    let hooks = Hooks::new();
    (Memory::default(), SingleVersionLock::new(Memory::new(), hooks.clone()), hooks)
}

/// Convenience function to create LMDB storage
pub fn lmdb(path: &Path) -> (Lmdb, SingleVersionLock<Lmdb>, Hooks) {
    let hooks = Hooks::new();
    let result = Lmdb::new(path);
    (result.clone(), SingleVersionLock::new(result, hooks.clone()), hooks)
}

/// Convenience function to create SQLite storage
pub fn sqlite(config: SqliteConfig) -> (Sqlite, SingleVersionLock<Sqlite>, Hooks) {
    let hooks = Hooks::new();
    let result = Sqlite::new(config);
    (result.clone(), SingleVersionLock::new(result, hooks.clone()), hooks)
}

/// Main ReifyDB convenience API
///
/// Provides simple static methods to create different types of database configurations
pub struct ReifyDB {}

impl ReifyDB {
    /// Create a new sync database with default configuration (serializable + memory)
    pub fn new_sync()
    -> SyncBuilder<Serializable<Memory, SingleVersionLock<Memory>>, SingleVersionLock<Memory>> {
        let (versioned, unversioned, hooks) = serializable(memory());
        SyncBuilder::new(versioned, unversioned, hooks)
    }

    /// Create a new sync database with custom storage and transaction layers
    pub fn new_sync_with<VT, UT>(input: (VT, UT, Hooks)) -> SyncBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        SyncBuilder::new(versioned, unversioned, hooks)
    }

    /// Create a new async database with default configuration (serializable + memory)
    #[cfg(feature = "async")]
    pub fn new_async()
    -> AsyncBuilder<Serializable<Memory, SingleVersionLock<Memory>>, SingleVersionLock<Memory>>
    {
        let (versioned, unversioned, hooks) = serializable(memory());
        AsyncBuilder::new(versioned, unversioned, hooks)
    }

    /// Create a new async database with custom storage and transaction layers
    #[cfg(feature = "async")]
    pub fn new_async_with<VT, UT>(input: (VT, UT, Hooks)) -> AsyncBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        AsyncBuilder::new(versioned, unversioned, hooks)
    }

    /// Create a new server with default configuration (serializable + memory)
    #[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
    pub fn new_server()
    -> ServerBuilder<Serializable<Memory, SingleVersionLock<Memory>>, SingleVersionLock<Memory>>
    {
        let (versioned, unversioned, hooks) = serializable(memory());
        ServerBuilder::new(versioned, unversioned, hooks)
    }

    /// Create a new server with custom storage and transaction layers
    #[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
    pub fn new_server_with<VT, UT>(input: (VT, UT, Hooks)) -> ServerBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        ServerBuilder::new(versioned, unversioned, hooks)
    }
}
