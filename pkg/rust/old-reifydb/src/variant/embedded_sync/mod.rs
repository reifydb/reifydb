// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::EmbeddedSyncBuilder;

use crate::hook::WithHooks;
use crate::session::{CommandSession, IntoCommandSession, IntoQuerySession, QuerySession, Session};
#[cfg(feature = "embedded_sync")]
use crate::session::SessionSync;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;

pub struct EmbeddedSync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub engine: Engine<VT, UT>,
}

impl<VT, UT> Clone for EmbeddedSync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<VT, UT> EmbeddedSync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> crate::Result<Self> {
        Ok(Self { engine: Engine::new(versioned, unversioned, hooks)? })
    }
}

impl<VT, UT> WithHooks<VT, UT> for EmbeddedSync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}

impl<VT, UT> Session<VT, UT> for EmbeddedSync<VT, UT>
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

#[cfg(feature = "embedded_sync")]
impl<VT, UT> SessionSync<VT, UT> for EmbeddedSync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{}
