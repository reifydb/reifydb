// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{DB, IntoSessionRx, IntoSessionTx, SessionRx, SessionTx};
use auth::Principal;
use engine::Engine;
use engine::execute::ExecutionResult;
use storage::StorageEngine;
use transaction::TransactionEngine;

pub struct Embedded<S: StorageEngine, T: TransactionEngine<S>> {
    engine: Engine<S, T>,
}

impl<S: StorageEngine, T: TransactionEngine<S>> Embedded<S, T> {
    pub fn new(transaction: T) -> (Self, Principal) {
        let principal = Principal::System { id: 1, name: "root".to_string() };

        (Self { engine: Engine::new(transaction) }, principal)
    }
}

impl<'a, S: StorageEngine, T: TransactionEngine<S>> DB<'a> for Embedded<S, T> {
    fn tx_execute_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        self.engine.tx_as(principal, &rql).unwrap()
    }

    fn rx_execute_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        self.engine.rx_as(&Principal::System { id: 1, name: "root".to_string() }, &rql).unwrap()
    }

    fn session_read_only(
        &self,
        into: impl IntoSessionRx<'a, Self>,
    ) -> base::Result<SessionRx<'a, Self>> {
        // into.into_session_rx(&self)
        todo!()
    }

    fn session(&self, into: impl IntoSessionTx<'a, Self>) -> base::Result<SessionTx<'a, Self>> {
        // into.into_session_tx(&self)
        todo!()
    }
}
