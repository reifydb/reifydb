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
pub mod system;
pub mod vendor;

pub use api::*;
pub use builder::{DatabaseBuilder, EmbeddedBuilder, ServerBuilder, WithInterceptorBuilder, WithSubsystem};
pub use context::{RuntimeProvider, SyncContext, SystemContext};
pub use database::Database;
pub use health::HealthMonitor;
pub use reifydb_auth as auth;
pub use reifydb_cdc as cdc;
pub use reifydb_core as core;
pub use reifydb_core::{event::EventBus, interface::auth::Identity};
pub use reifydb_derive as derive;
pub use reifydb_derive::FromFrame;
pub use reifydb_engine as engine;
pub use reifydb_rql as rql;
pub use reifydb_runtime::{
	SharedRuntime, SharedRuntimeConfig,
	actor::system::ActorSystem,
	clock::{Clock, MockClock},
};
pub use reifydb_store_multi as multi_storage;
pub use reifydb_store_multi::hot::{sqlite::config::SqliteConfig, storage::HotStorage};
pub use reifydb_store_single as single_storage;
// subsystems
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
pub use reifydb_sub_task as sub_task;
#[cfg(feature = "sub_tracing")]
pub use reifydb_sub_tracing as sub_tracing;
pub use reifydb_transaction as transaction;
pub use reifydb_transaction::{multi::transaction::TransactionMulti, single::svl::TransactionSvl};
pub use reifydb_type as r#type;
pub use reifydb_type::{
	Result,
	error::Error,
	params::Params,
	value,
	value::{
		Value,
		frame::{
			column::FrameColumn,
			data::FrameColumnData,
			extract::FrameError,
			frame::Frame,
			from_frame::{FromFrame, FromFrameError},
			row::{FrameRow, FrameRows},
		},
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		try_from::{FromValueError, TryFromValue, TryFromValueCoerce},
		r#type::Type,
	},
};
pub use session::{CommandSession, IntoCommandSession, QuerySession, Session};
