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
pub use query::QuerySession;
use reifydb_core::{
	Frame,
	interface::{Engine as EngineInterface, Identity, Params},
};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::SchedulerService;

pub trait Session {
	fn command_session(&self, session: impl IntoCommandSession) -> crate::Result<CommandSession>;

	fn query_session(&self, session: impl IntoQuerySession) -> crate::Result<QuerySession>;

	fn scheduler(&self) -> Option<SchedulerService>;

	fn command_as_root(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
		let session = self.command_session(Identity::root())?;
		session.command(rql, params)
	}

	fn query_as_root(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
		let session = self.query_session(Identity::root())?;
		session.query(rql, params)
	}
}

impl CommandSession {
	pub(crate) fn new(engine: StandardEngine, identity: Identity) -> Self {
		Self {
			engine,
			identity,
		}
	}

	pub fn command(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();
		self.engine.command_as(&self.identity, &rql, params).map_err(|mut err| {
			err.with_statement(rql);
			err
		})
	}
}

impl QuerySession {
	pub(crate) fn new(engine: StandardEngine, identity: Identity) -> Self {
		Self {
			engine,
			identity,
		}
	}

	pub fn query(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();
		self.engine.query_as(&self.identity, &rql, params).map_err(|mut err| {
			err.with_statement(rql);
			err
		})
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
