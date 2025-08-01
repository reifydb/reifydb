// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Frame;
use reifydb_core::interface::{
    Engine as _, NewTransaction, Principal, Transaction, UnversionedStorage, VersionedStorage,
};
use reifydb_engine::Engine;
use std::marker::PhantomData;

pub struct OnCreateContext<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: NewTransaction,
{
    pub engine: Engine<VS, US, T, UT>,
    _phantom: PhantomData<(VS, US, T, UT)>,
}

impl<'a, VS, US, T, UT> OnCreateContext<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: NewTransaction,
{
    pub fn new(engine: Engine<VS, US, T, UT>) -> Self {
        Self { engine, _phantom: PhantomData }
    }

    /// Execute a transactional query as the specified principal
    pub fn tx_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> Result<Vec<Frame>, reifydb_core::Error> {
        self.engine.tx_as(principal, rql)
    }

    /// Execute a transactional query as root user
    pub fn tx_as_root(&self, rql: &str) -> Result<Vec<Frame>, reifydb_core::Error> {
        let principal = Principal::System { id: 0, name: "root".to_string() };
        self.engine.tx_as(&principal, rql)
    }

    /// Execute a read-only query as the specified principal
    pub fn rx_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> Result<Vec<Frame>, reifydb_core::Error> {
        self.engine.rx_as(principal, rql)
    }

    /// Execute a read-only query as root user
    pub fn rx_as_root(&self, rql: &str) -> Result<Vec<Frame>, reifydb_core::Error> {
        let principal = Principal::root();
        self.engine.rx_as(&principal, rql)
    }
}
