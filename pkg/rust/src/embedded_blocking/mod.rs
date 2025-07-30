// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::result::Frame;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, Transaction, UnversionedStorage, VersionedStorage,
};
use reifydb_engine::Engine;

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
    pub fn new(transaction: T, hooks: Hooks) -> crate::Result<Self> {
        Ok(Self { engine: Engine::new(transaction, hooks)? })
    }
}

impl<'a, VS, US, T> Embedded<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn tx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        self.engine.tx_as(principal, rql).map_err(|mut err| {
            err.0.set_statement(rql.to_string());
            err
        })
    }

    pub fn tx_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.tx_as(&principal, rql)
    }

    pub fn rx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        self.engine.rx_as(principal, rql)
    }

    pub fn rx_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.rx_as(&principal, rql)
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
