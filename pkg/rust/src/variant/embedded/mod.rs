// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::EmbeddedBuilder;

use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::result::Frame;
use reifydb_engine::Engine;
use tokio::task::spawn_blocking;

pub struct Embedded<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
}

impl<VT, UT> Clone for Embedded<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<VT, UT> Embedded<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> Self {
        Self { engine: Engine::new(versioned, unversioned, hooks).unwrap() }
    }
}

impl<VT, UT> WithHooks<VT, UT> for Embedded<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}

impl<VT, UT> Embedded<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub async fn command_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let principal = principal.clone();

        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.command_as(&principal, &rql).map_err(|mut err| {
                err.set_statement(rql.to_string());
                err
            })
        })
        .await
        .unwrap()
    }

    pub async fn command_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.command_as(&principal, rql).await
    }

    pub async fn query_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let rql = rql.to_string();
        let principal = principal.clone();

        let engine = self.engine.clone();
        spawn_blocking(move || {
            engine.query_as(&principal, &rql).map_err(|mut err| {
                err.set_statement(rql.to_string());
                err
            })
        })
        .await
        .unwrap()
    }

    pub async fn query_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.query_as(&principal, rql).await
    }
}
