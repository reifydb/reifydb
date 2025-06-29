// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{DB, Error};
use reifydb_auth::Principal;
use reifydb_engine::{Engine, ExecutionResult};
use reifydb_storage::Storage;
use reifydb_transaction::Transaction;
use tokio::task::spawn_blocking;

pub struct Embedded<S: Storage + 'static, T: Transaction<S, S> + 'static> {
    engine: Engine<S, S, T>,
}

impl<S, T> Clone for Embedded<S, T>
where
    S: Storage,
    T: Transaction<S, S>,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<S: Storage, T: Transaction<S, S>> Embedded<S, T> {
    pub fn new(transaction: T) -> (Self, Principal) {
        let principal = Principal::System { id: 1, name: "root".to_string() };
        (Self { engine: Engine::new(transaction).unwrap() }, principal)
    }
}

impl<S: Storage + 'static, T: Transaction<S, S> + 'static> DB<'_> for Embedded<S, T> {
    async fn tx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        let rql = rql.to_string();
        let principal = principal.clone();

        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.tx_as(&principal, &rql).map_err(|err| {
                let diagnostic = err.diagnostic();
                Error::ExecutionError { diagnostic, source: rql.to_string() }
            })
        })
        .await
        .unwrap()
    }

    async fn rx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        let rql = rql.to_string();
        let principal = principal.clone();

        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.rx_as(&principal, &rql).map_err(|err| {
                let diagnostic = err.diagnostic();
                Error::ExecutionError { diagnostic, source: rql.to_string() }
            })
        })
        .await
        .unwrap()
    }

    // fn session_read_only(
    //     &self,
    //     into: impl IntoSessionRx<'a, Self>,
    // ) -> reifydb-core::Result<SessionRx<'a, Self>> {
    //     // into.into_session_rx(&self)
    //     todo!()
    // }
    //
    // fn session(&self, into: impl IntoSessionTx<'a, Self>) -> reifydb-core::Result<SessionTx<'a, Self>> {
    //     // into.into_session_tx(&self)
    //     todo!()
    // }
}
