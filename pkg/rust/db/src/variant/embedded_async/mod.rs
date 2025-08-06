// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::EmbeddedAsyncBuilder;

use crate::hook::WithHooks;
#[cfg(feature = "embedded_async")]
use crate::session::SessionAsync;
use crate::session::{CommandSession, IntoCommandSession, IntoQuerySession, QuerySession, Session};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;

pub struct EmbeddedAsync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
}

impl<VT, UT> Clone for EmbeddedAsync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<VT, UT> EmbeddedAsync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> Self {
        Self { engine: Engine::new(versioned, unversioned, hooks).unwrap() }
    }
}

impl<VT, UT> WithHooks<VT, UT> for EmbeddedAsync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}

impl<VT, UT> Session<VT, UT> for EmbeddedAsync<VT, UT>
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

#[cfg(feature = "embedded_async")]
impl<VT, UT> SessionAsync<VT, UT> for EmbeddedAsync<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
}
