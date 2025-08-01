// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::EmbeddedBlockingBuilder;

use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, UnversionedTransaction,
    VersionedTransaction,
};
use reifydb_core::result::Frame;
use reifydb_engine::Engine;

pub struct EmbeddedBlocking<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
}

impl<VT, UT> Clone for EmbeddedBlocking<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<VT, UT> EmbeddedBlocking<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> crate::Result<Self> {
        Ok(Self { engine: Engine::new(versioned, unversioned, hooks)? })
    }
}

impl<VT, UT> WithHooks<VT, UT> for EmbeddedBlocking<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}

impl<'a, VT, UT> EmbeddedBlocking<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn write_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        self.engine.write_as(principal, rql).map_err(|mut err| {
            err.0.set_statement(rql.to_string());
            err
        })
    }

    pub fn write_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.write_as(&principal, rql)
    }

    pub fn read_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        self.engine.read_as(principal, rql)
    }

    pub fn read_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.read_as(&principal, rql)
    }
}
