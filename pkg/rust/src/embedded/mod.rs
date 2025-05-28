// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::DB;
use reifydb_auth::Principal;
use reifydb_engine::{Engine, ExecutionResult};
use reifydb_persistence::Persistence;
use reifydb_transaction::Transaction;
use tokio::task::spawn_blocking;

pub struct Embedded<P: Persistence + 'static, T: Transaction<P> + 'static> {
    reifydb_engine: Engine<P, T>,
}

impl<P, T> Clone for Embedded<P, T>
where
    P: Persistence,
    T: Transaction<P>,
{
    fn clone(&self) -> Self {
        Self { reifydb_engine: self.reifydb_engine.clone() }
    }
}

impl<P: Persistence, T: Transaction<P>> Embedded<P, T> {
    pub fn new(transaction: T) -> (Self, Principal) {
        let principal = Principal::System { id: 1, name: "root".to_string() };

        (Self { reifydb_engine: Engine::new(transaction) }, principal)
    }
}

impl<'a, P: Persistence + 'static, T: Transaction<P> + 'static> DB<'a> for Embedded<P, T> {
    async fn tx_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let rql = rql.to_string();
        let principal = principal.clone();
        let reifydb_engine = self.reifydb_engine.clone();
        spawn_blocking(move || {
            let result = reifydb_engine.tx_as(&principal, &rql).unwrap();

            result
        })
        .await
        .unwrap()
    }

    async fn rx_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let rql = rql.to_string();
        let principal = principal.clone();
        let reifydb_engine = self.reifydb_engine.clone();
        spawn_blocking(move || {
            let result = reifydb_engine.rx_as(&principal, &rql).unwrap();
            result
        })
        .await
        .unwrap()
    }

    // fn session_read_only(
    //     &self,
    //     into: impl IntoSessionRx<'a, Self>,
    // ) -> reifydb_core::Result<SessionRx<'a, Self>> {
    //     // into.into_session_rx(&self)
    //     todo!()
    // }
    //
    // fn session(&self, into: impl IntoSessionTx<'a, Self>) -> reifydb_core::Result<SessionTx<'a, Self>> {
    //     // into.into_session_tx(&self)
    //     todo!()
    // }
}
