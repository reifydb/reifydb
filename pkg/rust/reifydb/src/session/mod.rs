// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Session management for ReifyDB
//!
//! Provides session-based access to the database engine with different
//! execution modes (sync/async) and permission levels.

#[allow(dead_code)]
mod command;
#[allow(dead_code)]
mod query;

pub use command::CommandSession;
pub use query::QuerySession;
use reifydb_core::{
	Frame,
	interface::{Identity, Params, Transaction},
};
use reifydb_engine::StandardEngine;

pub trait Session<T: Transaction> {
	fn command_session(
		&self,
		session: impl IntoCommandSession<T>,
	) -> crate::Result<CommandSession<T>>;

	fn query_session(
		&self,
		session: impl IntoQuerySession<T>,
	) -> crate::Result<QuerySession<T>>;
}

pub trait SessionSync<T: Transaction>: Session<T> {
	fn command_as_root(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> crate::Result<Vec<Frame>> {
		let session = self.command_session(Identity::root())?;
		session.command_sync(rql, params)
	}

	fn query_as_root(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> crate::Result<Vec<Frame>> {
		let session = self.query_session(Identity::root())?;
		session.query_sync(rql, params)
	}
}

#[cfg(feature = "async")]
pub trait SessionAsync<T: Transaction>: Session<T> + Sync {
	fn command_as_root(
		&self,
		rql: &str,
		params: impl Into<Params> + Send,
	) -> impl Future<Output = crate::Result<Vec<Frame>>> + Send {
		async {
			let session = self.command_session(Identity::root())?;
			session.command_async(rql, params).await
		}
	}

	fn query_as_root(
		&self,
		rql: &str,
		params: impl Into<Params> + Send,
	) -> impl Future<Output = crate::Result<Vec<Frame>>> + Send {
		async {
			let session = self.query_session(Identity::root())?;
			session.query_async(rql, params).await
		}
	}
}

pub trait IntoCommandSession<T: Transaction> {
	fn into_command_session(
		self,
		engine: StandardEngine<T>,
	) -> crate::Result<CommandSession<T>>;
}

pub trait IntoQuerySession<T: Transaction> {
	fn into_query_session(
		self,
		engine: StandardEngine<T>,
	) -> crate::Result<QuerySession<T>>;
}

impl<T: Transaction> IntoCommandSession<T> for Identity {
	fn into_command_session(
		self,
		engine: StandardEngine<T>,
	) -> crate::Result<CommandSession<T>> {
		Ok(CommandSession::new(engine, self))
	}
}

impl<T: Transaction> IntoQuerySession<T> for Identity {
	fn into_query_session(
		self,
		engine: StandardEngine<T>,
	) -> crate::Result<QuerySession<T>> {
		Ok(QuerySession::new(engine, self))
	}
}
