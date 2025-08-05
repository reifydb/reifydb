// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::EmbeddedBlockingBuilder;

use crate::hook::WithHooks;
// use crate::session::{QuerySessionBuilder, CommandSessionBuilder};
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
    pub fn command_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        self.engine.command_as(principal, rql).map_err(|mut err| {
            err.set_statement(rql.to_string());
            err
        })
    }

    pub fn command_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.command_as(&principal, rql)
    }

    pub fn query_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        self.engine.query_as(principal, rql)
    }

    pub fn query_as_root(&self, rql: &str) -> crate::Result<Vec<Frame>> {
        let principal = Principal::root();
        self.query_as(&principal, rql)
    }
    
    // pub fn query_session(&self, principal: Principal) -> QuerySessionBuilder<VT, UT> {
    //     QuerySessionBuilder::new(&self.engine as &dyn EngineInterface<VT, UT>, principal)
    // }
    //
    // pub fn command_session(&self, principal: Principal) -> CommandSessionBuilder<VT, UT> {
    //     CommandSessionBuilder::new(&self.engine as &dyn EngineInterface<VT, UT>, principal)
    // }
}
