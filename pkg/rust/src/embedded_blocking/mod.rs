// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_auth::Principal;
use reifydb_engine::{Engine, ExecutionResult};
use reifydb_persistence::Persistence;
use reifydb_transaction::{catalog_init, Transaction};

pub struct Embedded<P: Persistence + 'static, T: Transaction<P> + 'static> {
    engine: Engine<P, T>,
}

impl<P, T> Clone for Embedded<P, T>
where
    P: Persistence,
    T: Transaction<P>,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<P: Persistence, T: Transaction<P>> Embedded<P, T> {
    pub fn new(transaction: T) -> (Self, Principal) {
        let principal = Principal::System { id: 1, name: "root".to_string() };
        catalog_init();
        (Self { engine: Engine::new(transaction) }, principal)
    }
}

impl<'a, P: Persistence + 'static, T: Transaction<P> + 'static> Embedded<P, T> {
    pub fn tx_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let result = self.engine.tx_as(principal, rql).unwrap();
        result
    }

    pub fn rx_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let result = self.engine.rx_as(&principal, &rql).unwrap();
        result
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
