// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
    Engine as EngineInterface, Params, Principal, Transaction,
};
use reifydb_core::result::Frame;
use reifydb_engine::Engine;
#[cfg(feature = "async")]
use tokio::task::spawn_blocking;

/// Session for executing read-only database queries
pub struct QuerySession<T: Transaction> {
    pub(crate) engine: Engine<T>,
    pub(crate) principal: Principal,
}

impl<T: Transaction> QuerySession<T> {
    pub(crate) fn new(engine: Engine<T>, principal: Principal) -> Self {
        Self { engine, principal }
    }

    /// Execute a synchronous query
    pub fn query_sync(&self, rql: &str, params: impl Into<Params>) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();
        self.engine.query_as(&self.principal, &rql, params).map_err(|mut err| {
            err.set_statement(rql);
            err
        })
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