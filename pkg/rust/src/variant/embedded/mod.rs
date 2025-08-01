// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::EmbeddedBuilder;

use crate::DB;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    Engine as EngineInterface, NewTransaction, Principal, Transaction, UnversionedStorage, VersionedStorage,
};
use reifydb_core::result::Frame;
use reifydb_engine::Engine;
use tokio::task::spawn_blocking;

pub struct Embedded<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: NewTransaction,
{
    engine: Engine<VS, US, T, UT>,
}

impl<VS, US, T, UT> Clone for Embedded<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: NewTransaction,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<VS, US, T, UT> Embedded<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: NewTransaction,
{
    pub fn new(transaction: T, unversioned: UT, hooks: Hooks) -> Self {
        Self { engine: Engine::new(transaction, unversioned, hooks).unwrap() }
    }
}

impl<VS, US, T, UT> WithHooks<VS, US, T, UT> for Embedded<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: NewTransaction,
{
    fn engine(&self) -> &Engine<VS, US, T, UT> {
        &self.engine
    }
}

impl<VS, US, T, UT> DB<'_> for Embedded<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: NewTransaction,
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

    async fn tx_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.tx_as(&principal, rql).await
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

    async fn rx_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.rx_as(&principal, rql).await
    }
}
