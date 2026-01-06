// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Session management for ReifyDB
//!
//! Provides session-based access to the database engine with different
//! permission levels.

#[allow(dead_code)]
mod command;
#[allow(dead_code)]
mod query;

pub use command::CommandSession;
pub use query::QuerySession;
use reifydb_core::{
	Frame,
	interface::{Identity, Params},
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
	pub fn command(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_type::Error> {
		self.engine.command_as(&self.identity, rql, params.into())
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
	pub fn query(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_type::Error> {
		self.engine.query_as(&self.identity, rql, params.into())
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
