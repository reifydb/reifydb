// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{DB, IntoSessionRx, IntoSessionTx, SessionRx, SessionTx};
use auth::Principal;
use engine::Engine;
use engine::execute::{ExecutionResult, execute_plan, execute_plan_mut};
use rql::ast;
use rql::plan::{plan, plan_mut};
use storage::StorageEngine;
use transaction::{Rx, TransactionEngine, Tx};

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
    fn tx_execute_as(&self, _principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let mut tx = self.engine.begin().unwrap();

        for statement in statements {
            let plan = plan_mut(tx.catalog().unwrap(), statement).unwrap();
            let er = execute_plan_mut(plan, &mut tx).unwrap();
            result.push(er);
        }

        tx.commit().unwrap();

        result
    }

    fn rx_execute_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let rx = self.engine.begin_read_only().unwrap();
        for statement in statements {
            let plan = plan(statement).unwrap();
            let er = execute_plan(plan, &rx).unwrap();
            result.push(er);
        }

        result
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
