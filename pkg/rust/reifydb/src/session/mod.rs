// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Session management for ReifyDB
//!
//! Provides session-based access to the database engine with different
//! permission levels.

#[allow(dead_code)]
mod command;
#[allow(dead_code)]
mod query;

pub use command::CommandSession;
use futures_util::TryStreamExt;
pub use query::QuerySession;
use reifydb_core::{
	Frame,
	interface::{Engine as EngineInterface, Identity, Params},
	stream::StreamError,
};
use reifydb_engine::StandardEngine;
use tracing::instrument;

pub trait Session {
	fn command_session(&self, session: impl IntoCommandSession) -> crate::Result<CommandSession>;

	fn query_session(&self, session: impl IntoQuerySession) -> crate::Result<QuerySession>;
}

impl CommandSession {
	#[instrument(name = "api::session::command::new", level = "debug", skip_all)]
	pub(crate) fn new(engine: StandardEngine, identity: Identity) -> Self {
		Self {
			engine,
			identity,
		}
	}

	#[instrument(name = "api::session::command", level = "info", skip(self, params), fields(rql = %rql))]
	pub async fn command(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, StreamError> {
		let rql = rql.to_string();
		let params = params.into();
		self.engine.command_as(&self.identity, &rql, params).try_collect().await
	}
}

impl QuerySession {
	#[instrument(name = "api::session::query::new", level = "debug", skip_all)]
	pub(crate) fn new(engine: StandardEngine, identity: Identity) -> Self {
		Self {
			engine,
			identity,
		}
	}

	#[instrument(name = "api::session::query", level = "info", skip(self, params), fields(rql = %rql))]
	pub async fn query(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, StreamError> {
		let rql = rql.to_string();
		let params = params.into();
		self.engine.query_as(&self.identity, &rql, params).try_collect().await
	}
}

pub trait IntoCommandSession {
	fn into_command_session(self, engine: StandardEngine) -> crate::Result<CommandSession>;
}

pub trait IntoQuerySession {
	fn into_query_session(self, engine: StandardEngine) -> crate::Result<QuerySession>;
}

impl IntoCommandSession for Identity {
	fn into_command_session(self, engine: StandardEngine) -> crate::Result<CommandSession> {
		Ok(CommandSession::new(engine, self))
	}
}

impl IntoQuerySession for Identity {
	fn into_query_session(self, engine: StandardEngine) -> crate::Result<QuerySession> {
		Ok(QuerySession::new(engine, self))
	}
}
