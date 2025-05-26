// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use auth::Principal;
use engine::Engine;
use engine::old_execute::ExecutionResult;
use storage::StorageEngine;
use transaction::TransactionEngine;

pub struct Embedded<S: StorageEngine + 'static, T: TransactionEngine<S> + 'static> {
    engine: Engine<S, T>,
}

impl<S, T> Clone for Embedded<S, T>
where
    S: StorageEngine,
    T: TransactionEngine<S>,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<S: StorageEngine, T: TransactionEngine<S>> Embedded<S, T> {
    pub fn new(transaction: T) -> (Self, Principal) {
        let principal = Principal::System { id: 1, name: "root".to_string() };

        (Self { engine: Engine::new(transaction) }, principal)
    }
}

impl<'a, S: StorageEngine + 'static, T: TransactionEngine<S> + 'static> Embedded<S, T> {
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
    // ) -> base::Result<SessionRx<'a, Self>> {
    //     // into.into_session_rx(&self)
    //     todo!()
    // }
    //
    // fn session(&self, into: impl IntoSessionTx<'a, Self>) -> base::Result<SessionTx<'a, Self>> {
    //     // into.into_session_tx(&self)
    //     todo!()
    // }
}
