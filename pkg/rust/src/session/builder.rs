// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::session::{CommandSession, QuerySession};
use reifydb_core::interface::{Principal, UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;

pub struct QuerySessionBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
    principal: Principal,
}

impl<VT, UT> QuerySessionBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(engine: Engine<VT, UT>, principal: Principal) -> Self {
        Self { engine, principal }
    }

    pub fn build(self) -> QuerySession<VT, UT> {
        QuerySession::new(self.engine.clone(), self.principal)
    }
}

pub struct CommandSessionBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
    principal: Principal,
}

impl<VT, UT> CommandSessionBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(engine: Engine<VT, UT>, principal: Principal) -> Self {
        Self { engine, principal }
    }

    pub fn build(self) -> CommandSession<VT, UT> {
        CommandSession::new(self.engine, self.principal)
    }
}
