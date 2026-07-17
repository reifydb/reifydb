// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

mod api;
mod boot;
mod builder;
mod context;
mod database;
pub mod event;
mod health;
#[cfg(feature = "sub_raft")]
pub mod raft;
mod session;
#[cfg(feature = "sub_flow")]
mod subscribe;
pub mod subsystem;
pub mod system;
pub mod vendor;
pub mod watermarks;

pub use api::{
	migration::{Migration, MigrationSource, MigrationStatement},
	*,
};
pub use builder::{
	DatabaseBuilder, EmbeddedBuilder, InterceptBuilder, ServerBuilder, WithInterceptorBuilder, WithSubsystem,
};
pub use context::{RuntimeProvider, SyncContext, SystemContext};
pub use database::Database;
pub use health::HealthMonitor;
pub use reifydb_abi as abi;
pub use reifydb_allocator as allocator;
pub use reifydb_auth as auth;
pub use reifydb_catalog as catalog;
pub use reifydb_cdc as cdc;
pub use reifydb_codec as codec;
pub use reifydb_column as column;
pub use reifydb_core as core;
#[cfg(feature = "sub_server")]
pub use reifydb_core::actors::server::Operation;
pub use reifydb_core::{
	event::EventBus,
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		subscription::HydrationConfig,
	},
};
pub use reifydb_derive as derive;
pub use reifydb_derive::FromFrame;
pub use reifydb_engine as engine;
pub use reifydb_export as export;
pub use reifydb_export::options::{ExportOptions, ShapeKind};
pub use reifydb_extension as extension;
pub use reifydb_macro as r#macro;
pub use reifydb_metric as metric;
pub use reifydb_policy as policy;
pub use reifydb_profiler as profiler;
pub use reifydb_remote_proxy as remote_proxy;
pub use reifydb_routine as routine;
pub use reifydb_routine::{function, procedure};
pub use reifydb_rql as rql;
pub use reifydb_runtime as runtime;
pub use reifydb_runtime::{
	Runtime, RuntimeConfig, RuntimeHandle,
	actor::{
		mailbox::ActorRef,
		system::{ActorSpawner, ActorSystem},
	},
	context::clock::{Clock, MockClock},
	pool::PoolConfig,
};
pub use reifydb_sdk as sdk;
pub use reifydb_sqlite as sqlite;
pub use reifydb_sqlite::SqliteConfig;
pub use reifydb_store_multi as multi_storage;
pub use reifydb_store_multi::tier::commit::buffer::MultiCommitBufferTier;
pub use reifydb_store_single as single_storage;
// subsystems
pub use reifydb_sub_api as sub;
#[cfg(feature = "sub_flow")]
pub use reifydb_sub_flow as sub_flow;
#[cfg(feature = "sub_flow")]
pub use reifydb_sub_flow::{
	builder::OperatorFactory,
	operator::stateful::{
		keyed::KeyedStateful, raw::RawStatefulOperator, row::RowNumberProvider, single::SingleStateful,
	},
	operator::{BoxedOperator, Operator, Operators},
	transaction::FlowTransaction,
};
pub use reifydb_sub_metric as sub_metric;
#[cfg(feature = "sub_raft")]
pub use reifydb_sub_raft as sub_raft;
#[cfg(feature = "sub_replication")]
pub use reifydb_sub_replication as sub_replication;
#[cfg(feature = "sub_server")]
pub use reifydb_sub_server as sub_server;
#[cfg(feature = "sub_server")]
pub use reifydb_sub_server::interceptor::{
	Protocol, RequestContext, RequestInterceptor, RequestInterceptorChain, RequestMetadata, ResponseContext,
};
#[cfg(feature = "sub_server_admin")]
pub use reifydb_sub_server_admin as sub_server_admin;
#[cfg(feature = "sub_server_grpc")]
pub use reifydb_sub_server_grpc as sub_server_grpc;
#[cfg(feature = "sub_server_http")]
pub use reifydb_sub_server_http as sub_server_http;
#[cfg(feature = "sub_server_otel")]
pub use reifydb_sub_server_otel as sub_server_otel;
#[cfg(feature = "sub_server_ws")]
pub use reifydb_sub_server_ws as sub_server_ws;
#[cfg(feature = "sub_flow")]
pub use reifydb_sub_subscription as sub_subscription;
#[cfg(not(reifydb_single_threaded))]
pub use reifydb_sub_task as sub_task;
#[cfg(feature = "sub_tracing")]
pub use reifydb_sub_tracing as sub_tracing;
pub use reifydb_subscription as subscription;
#[cfg(feature = "testing")]
pub use reifydb_testing as testing;
pub use reifydb_transaction as transaction;
pub use reifydb_transaction::{multi::transaction::MultiTransaction, single::SingleTransaction};
pub use reifydb_value as value;
pub use reifydb_value::{
	Result,
	error::Error,
	params::Params,
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
		identity::IdentityId,
		iso::{IsoDate, IsoDateTime, IsoDuration, IsoTime},
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		try_from::{FromValueError, TryFromValue, TryFromValueCoerce},
		value_type::ValueType,
	},
};
#[cfg(feature = "sub_flow")]
pub use subscribe::Subscription;
pub mod test;
pub use session::{Backoff, RetryStrategy, Session};
