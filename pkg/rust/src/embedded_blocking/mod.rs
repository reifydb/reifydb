// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;
use reifydb_auth::Principal;
use reifydb_core::interface::{Bypass, Storage, Transaction};
use reifydb_engine::{Engine, ExecutionResult};

pub struct Embedded<
    S: Storage + 'static,
    BP: Bypass<S> + 'static,
    T: Transaction<S, S, BP> + 'static,
> {
    engine: Engine<S, S, BP, T>,
}

impl<S, BP, T> Clone for Embedded<S, BP, T>
where
    S: Storage,
    BP: Bypass<S>,
    T: Transaction<S, S, BP>,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<S: Storage, BP: Bypass<S>, T: Transaction<S, S, BP>> Embedded<S, BP, T> {
    pub fn new(transaction: T) -> crate::Result<(Self, Principal)> {
        let principal = Principal::System { id: 1, name: "root".to_string() };
        Ok((Self { engine: Engine::new(transaction)? }, principal))
    }
}

impl<'a, S: Storage + 'static, BP: Bypass<S> + 'static, T: Transaction<S, S, BP> + 'static>
    Embedded<S, BP, T>
{
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
