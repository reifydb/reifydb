// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::session::RqlParams;
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::result::Frame;
use reifydb_engine::Engine;
#[cfg(feature = "embedded_async")]
use tokio::task::spawn_blocking;

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

    #[cfg(feature = "embedded_sync")]
    pub fn query_sync(&self, rql: &str, params: impl Into<RqlParams>) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();
        let substituted_rql = params.substitute(&rql)?;
        self.engine.query_as(&self.principal, &substituted_rql).map_err(|mut err| {
            err.set_statement(rql);
            err
        })
    }

    #[cfg(feature = "embedded_sync")]
    pub fn command_sync(
        &self,
        rql: &str,
        params: impl Into<RqlParams>,
    ) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();
        let substituted_rql = params.substitute(&rql)?;
        self.engine.command_as(&self.principal, &substituted_rql).map_err(|mut err| {
            err.set_statement(rql);
            err
        })
    }

    #[cfg(feature = "embedded_async")]
    pub async fn command_async(
        &self,
        rql: &str,
        params: impl Into<RqlParams>,
    ) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();
        let substituted_rql = params.substitute(&rql)?;

        let principal = self.principal.clone();
        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.command_as(&principal, &substituted_rql).map_err(|mut err| {
                err.set_statement(substituted_rql.to_string());
                err
            })
        })
        .await
        .unwrap()
    }

    #[cfg(feature = "embedded_async")]
    pub async fn query_async(
        &self,
        rql: &str,
        params: impl Into<RqlParams>,
    ) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let params = params.into();
        let substituted_rql = params.substitute(&rql)?;

        let principal = self.principal.clone();
        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.query_as(&principal, &substituted_rql).map_err(|mut err| {
                err.set_statement(substituted_rql.to_string());
                err
            })
        })
        .await
        .unwrap()
    }
}
