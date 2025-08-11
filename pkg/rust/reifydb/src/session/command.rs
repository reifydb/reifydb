// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
    Engine as EngineInterface, Params, Principal, UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::result::Frame;
use reifydb_engine::Engine;
#[cfg(feature = "async")]
use tokio::task::spawn_blocking;

/// Session for executing database commands (DDL/DML operations)
pub struct CommandSession<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) engine: Engine<VT, UT>,
    pub(crate) principal: Principal,
}

impl<VT, UT> CommandSession<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(engine: Engine<VT, UT>, principal: Principal) -> Self {
        Self { engine, principal }
    }

    /// Execute a synchronous query command
    pub fn query_sync(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();
        self.engine.query_as(&self.principal, &rql, params).map_err(|mut err| {
            err.set_statement(rql);
            err
        })
    }

    /// Execute a synchronous command
    pub fn command_sync(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();
        self.engine.command_as(&self.principal, &rql, params).map_err(|mut err| {
            err.set_statement(rql);
            err
        })
    }

    /// Execute an asynchronous command
    #[cfg(feature = "async")]
    pub async fn command_async(
        &self,
        rql: &str,
        params: impl Into<Params>,
    ) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();

        let principal = self.principal.clone();
        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.command_as(&principal, &rql, params).map_err(|mut err| {
                err.set_statement(rql.to_string());
                err
            })
        })
        .await
        .unwrap()
    }

    /// Execute an asynchronous query
    #[cfg(feature = "async")]
    pub async fn query_async(
        &self,
        rql: &str,
        params: impl Into<Params>,
    ) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();

        let principal = self.principal.clone();
        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.query_as(&principal, &rql, params).map_err(|mut err| {
                err.set_statement(rql.to_string());
                err
            })
        })
        .await
        .unwrap()
    }
}