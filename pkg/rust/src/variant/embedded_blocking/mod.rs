// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::EmbeddedBlockingBuilder;

use crate::hook::WithHooks;
use crate::session::{CommandSession, IntoCommandSession, IntoQuerySession, QuerySession, Session};
#[cfg(feature = "embedded_blocking")]
use crate::session::SessionSync;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
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

impl<VT, UT> Session<VT, UT> for EmbeddedBlocking<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn command_session(
        &self,
        session: impl IntoCommandSession<VT, UT>,
    ) -> crate::Result<CommandSession<VT, UT>> {
        session.into_command_session(self.engine.clone())
    }

    fn query_session(
        &self,
        session: impl IntoQuerySession<VT, UT>,
    ) -> crate::Result<QuerySession<VT, UT>> {
        session.into_query_session(self.engine.clone())
    }
}

#[cfg(feature = "embedded_blocking")]
impl<VT, UT> SessionSync<VT, UT> for EmbeddedBlocking<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{}
