// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod api;
mod boot;
mod builder;
mod context;
mod database;
pub mod event;
mod health;
mod session;
pub mod subsystem;
pub mod vendor;

pub use api::*;
pub use builder::{DatabaseBuilder, EmbeddedBuilder, ServerBuilder, WithInterceptorBuilder, WithSubsystem};
pub use context::{RuntimeProvider, SyncContext, SystemContext};
pub use database::Database;
pub use event::{OnCreateContext, WithEventBus};
pub use health::HealthMonitor;
pub use reifydb_auth as auth;
pub use reifydb_cdc as cdc;
// subsystems
pub use reifydb_core as core;
pub use reifydb_core::{
	ComputePool, Error, Result, SharedRuntime, SharedRuntimeConfig,
	event::EventBus,
	interface::{Identity, Params},
};
pub use reifydb_derive as derive;
pub use reifydb_derive::FromFrame;
pub use reifydb_engine as engine;
pub use reifydb_rql as rql;
pub use reifydb_store_multi as multi_storage;
pub use reifydb_store_single as single_storage;
pub use reifydb_store_multi::{hot::HotStorage, sqlite::SqliteConfig};
pub use reifydb_sub_api as sub;
#[cfg(feature = "sub_flow")]
pub use reifydb_sub_flow as sub_flow;
#[cfg(feature = "sub_server")]
pub use reifydb_sub_server as sub_server;
#[cfg(feature = "sub_server_admin")]
pub use reifydb_sub_server_admin as sub_server_admin;
#[cfg(feature = "sub_server_http")]
pub use reifydb_sub_server_http as sub_server_http;
#[cfg(feature = "sub_server_otel")]
pub use reifydb_sub_server_otel as sub_server_otel;
#[cfg(feature = "sub_server_ws")]
pub use reifydb_sub_server_ws as sub_server_ws;
#[cfg(feature = "sub_tracing")]
pub use reifydb_sub_tracing as sub_tracing;
pub use reifydb_transaction as transaction;
pub use reifydb_transaction::{multi::TransactionMulti, single::TransactionSvl};
pub use reifydb_type as r#type;
pub use reifydb_type::{
	Frame, FrameColumn, FrameColumnData, FrameError, FrameRow, FrameRows, FromFrame, FromFrameError,
	FromValueError, OrderedF32, OrderedF64, TryFromValue, TryFromValueCoerce, Type, Value,
};
pub use session::{CommandSession, IntoCommandSession, QuerySession, Session};
