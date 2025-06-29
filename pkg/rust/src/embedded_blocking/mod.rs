// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;
use reifydb_auth::Principal;
use reifydb_core::interface::Storage;
use reifydb_engine::{Engine, ExecutionResult};
use reifydb_transaction::Transaction;

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
    pub fn new(transaction: T) -> crate::Result<(Self, Principal)> {
        let principal = Principal::System { id: 1, name: "root".to_string() };
        Ok((Self { engine: Engine::new(transaction)? }, principal))
    }
}

impl<'a, S: Storage + 'static, T: Transaction<S, S> + 'static> Embedded<S, T> {
    pub fn tx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        self.engine.tx_as(principal, rql).map_err(|err| {
            let diagnostic = err.diagnostic();
            Error::ExecutionError { diagnostic, source: rql.to_string() }
        })
    }

    pub fn rx_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        self.engine.rx_as(principal, rql).unwrap()
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
