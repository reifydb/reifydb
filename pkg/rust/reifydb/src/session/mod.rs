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
use reifydb_core::Frame;
use reifydb_core::interface::{Params, Principal, UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;
use std::future::Future;

pub trait Session<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn command_session(
        &self,
        session: impl IntoCommandSession<VT, UT>,
    ) -> crate::Result<CommandSession<VT, UT>>;

    fn query_session(
        &self,
        session: impl IntoQuerySession<VT, UT>,
    ) -> crate::Result<QuerySession<VT, UT>>;
}

pub trait SessionSync<VT, UT>: Session<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn command_as_root(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
        let session = self.command_session(Principal::root())?;
        session.command_sync(rql, params)
    }

    fn query_as_root(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
        let session = self.query_session(Principal::root())?;
        session.query_sync(rql, params)
    }
}

#[cfg(feature = "async")]
pub trait SessionAsync<VT, UT>: Session<VT, UT> + Sync
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn command_as_root(
        &self,
        rql: &str,
        params: impl Into<Params> + Send,
    ) -> impl Future<Output = crate::Result<Vec<Frame>>> + Send {
        async {
            let session = self.command_session(Principal::root())?;
            session.command_async(rql, params).await
        }
    }

    fn query_as_root(
        &self,
        rql: &str,
        params: impl Into<Params> + Send,
    ) -> impl Future<Output = crate::Result<Vec<Frame>>> + Send {
        async {
            let session = self.query_session(Principal::root())?;
            session.query_async(rql, params).await
        }
    }
}

pub trait IntoCommandSession<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn into_command_session(self, engine: Engine<VT, UT>) -> crate::Result<CommandSession<VT, UT>>;
}

pub trait IntoQuerySession<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn into_query_session(self, engine: Engine<VT, UT>) -> crate::Result<QuerySession<VT, UT>>;
}

impl<VT, UT> IntoCommandSession<VT, UT> for Principal
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn into_command_session(self, engine: Engine<VT, UT>) -> crate::Result<CommandSession<VT, UT>> {
        Ok(CommandSession::new(engine, self))
    }
}

impl<VT, UT> IntoQuerySession<VT, UT> for Principal
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn into_query_session(self, engine: Engine<VT, UT>) -> crate::Result<QuerySession<VT, UT>> {
        Ok(QuerySession::new(engine, self))
    }
}
