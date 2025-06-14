// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;
use reifydb_auth::Principal;
use reifydb_engine::{Engine, ExecutionResult};
use reifydb_storage::VersionedStorage;
use reifydb_transaction::{Transaction, catalog_init};

pub struct Embedded<VS: VersionedStorage + 'static, T: Transaction<VS> + 'static> {
    engine: Engine<VS, T>,
}

impl<VS, T> Clone for Embedded<VS, T>
where
    VS: VersionedStorage,
    T: Transaction<VS>,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<VS: VersionedStorage, T: Transaction<VS>> Embedded<VS, T> {
    pub fn new(transaction: T) -> (Self, Principal) {
        let principal = Principal::System { id: 1, name: "root".to_string() };
        catalog_init();
        (Self { engine: Engine::new(transaction) }, principal)
    }
}

impl<'a, VS: VersionedStorage + 'static, T: Transaction<VS> + 'static> Embedded<VS, T> {
    pub fn tx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        self.engine.tx_as(principal, rql).map_err(|err| {
            let diagnostic = err.diagnostic();
            Error { diagnostic, source: rql.to_string() }
        })
    }

    pub fn rx_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let result = self.engine.rx_as(&principal, &rql).unwrap();
        result
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
