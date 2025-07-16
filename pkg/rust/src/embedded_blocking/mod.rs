// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Error;
use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use reifydb_engine::frame::Frame;

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
    pub fn new(transaction: T) -> crate::Result<(Self, Principal)> {
        let principal = Principal::System { id: 1, name: "root".to_string() };
        Ok((Self { engine: Engine::new(transaction)? }, principal))
    }
}

impl<'a, VS, US, T> Embedded<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn tx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        self.engine.tx_as(principal, rql).map_err(|err| {
            let mut diagnostic = err.diagnostic();
            diagnostic.set_statement(rql.to_string());
            Error::ExecutionError { diagnostic }
        })
    }

    pub fn rx_as(&self, principal: &Principal, rql: &str) -> Vec<Frame> {
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
