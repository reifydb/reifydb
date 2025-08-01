// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::EmbeddedBlocking;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::hook::lifecycle::OnInitHook;
use reifydb_core::interface::{GetHooks, UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;

pub struct EmbeddedBlockingBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
}

impl<VT, UT> EmbeddedBlockingBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> Self {
        Self { engine: Engine::new(versioned, unversioned, hooks).unwrap() }
    }

    pub fn build(self) -> EmbeddedBlocking<VT, UT> {
        self.engine.get_hooks().trigger(OnInitHook {}).unwrap();
        EmbeddedBlocking { engine: self.engine }
    }
}

impl<VT, UT> WithHooks<VT, UT> for EmbeddedBlockingBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}
