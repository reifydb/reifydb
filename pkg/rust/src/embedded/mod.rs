// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::DB;
use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use reifydb_core::frame::Frame;
use tokio::task::spawn_blocking;

pub struct Embedded<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    engine: Engine<VS, US, T>,
}

impl<VS, US, T> Clone for Embedded<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<VS, US, T> Embedded<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(transaction: T) -> (Self, Principal) {
        let principal = Principal::System { id: 1, name: "root".to_string() };
        (Self { engine: Engine::new(transaction).unwrap() }, principal)
    }
}

impl<VS, US, T> DB<'_> for Embedded<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    async fn tx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let principal = principal.clone();

        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.tx_as(&principal, &rql).map_err(|mut err| {
                err.0.set_statement(rql.to_string());
                err
            })
        })
        .await
        .unwrap()
    }

    async fn rx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let principal = principal.clone();

        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.rx_as(&principal, &rql).map_err(|mut err| {
                err.0.set_statement(rql.to_string());
                err
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
