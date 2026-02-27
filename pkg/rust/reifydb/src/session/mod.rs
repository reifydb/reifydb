// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Session management for ReifyDB
//!
//! Provides session-based access to the database engine with different
//! permission levels.

#[allow(dead_code)]
mod admin;
#[allow(dead_code)]
mod command;
#[allow(dead_code)]
mod query;

pub use admin::AdminSession;
pub use command::CommandSession;
pub use query::QuerySession;
use reifydb_engine::engine::StandardEngine;
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};
use tracing::instrument;

pub trait Session {
	fn admin_session(&self, session: impl IntoAdminSession) -> crate::Result<AdminSession>;

	fn command_session(&self, session: impl IntoCommandSession) -> crate::Result<CommandSession>;

	fn query_session(&self, session: impl IntoQuerySession) -> crate::Result<QuerySession>;
}

impl AdminSession {
	#[instrument(name = "api::session::admin::new", level = "debug", skip_all)]
	pub(crate) fn new(engine: StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
		}
	}

	#[instrument(name = "api::session::admin", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn admin(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_type::error::Error> {
		self.engine.admin_as(self.identity, rql, params.into())
	}
}

impl CommandSession {
	#[instrument(name = "api::session::command::new", level = "debug", skip_all)]
	pub(crate) fn new(engine: StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
		}
	}

	#[instrument(name = "api::session::command", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn command(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_type::error::Error> {
		self.engine.command_as(self.identity, rql, params.into())
	}
}

impl QuerySession {
	#[instrument(name = "api::session::query::new", level = "debug", skip_all)]
	pub(crate) fn new(engine: StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
		}
	}

	#[instrument(name = "api::session::query", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn query(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_type::error::Error> {
		self.engine.query_as(self.identity, rql, params.into())
	}
}

pub trait IntoAdminSession {
	fn into_admin_session(self, engine: StandardEngine) -> crate::Result<AdminSession>;
}

pub trait IntoCommandSession {
	fn into_command_session(self, engine: StandardEngine) -> crate::Result<CommandSession>;
}

pub trait IntoQuerySession {
	fn into_query_session(self, engine: StandardEngine) -> crate::Result<QuerySession>;
}

impl IntoAdminSession for IdentityId {
	fn into_admin_session(self, engine: StandardEngine) -> crate::Result<AdminSession> {
		Ok(AdminSession::new(engine, self))
	}
}

impl IntoCommandSession for IdentityId {
	fn into_command_session(self, engine: StandardEngine) -> crate::Result<CommandSession> {
		Ok(CommandSession::new(engine, self))
	}
}

impl IntoQuerySession for IdentityId {
	fn into_query_session(self, engine: StandardEngine) -> crate::Result<QuerySession> {
		Ok(QuerySession::new(engine, self))
	}
}
