// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{DB, Error};
use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use reifydb_engine::frame::Frame;
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
    async fn execute_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let principal = principal.clone();

        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.execute_as(&principal, &rql).map_err(|err| {
                let mut diagnostic = err.diagnostic();
                diagnostic.statement = Some(rql.to_string());
                Error::ExecutionError { diagnostic }
            })
        })
        .await
        .unwrap()
    }

    async fn query_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let principal = principal.clone();

        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.query_as(&principal, &rql).map_err(|err| {
                let mut diagnostic = err.diagnostic();
                diagnostic.statement = Some(rql.to_string());
                Error::ExecutionError { diagnostic }
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
