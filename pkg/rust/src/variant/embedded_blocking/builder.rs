// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::EmbeddedBlocking;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::hook::lifecycle::OnInitHook;
use reifydb_core::interface::{GetHooks, UnversionedTransaction, VersionedTransaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;

pub struct EmbeddedBlockingBuilder<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    engine: Engine<VS, US, T, UT>,
}

impl<VS, US, T, UT> EmbeddedBlockingBuilder<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn new(transaction: T, unversioned: UT, hooks: Hooks) -> Self {
        Self { engine: Engine::new(transaction, unversioned, hooks).unwrap() }
    }

    pub fn build(self) -> EmbeddedBlocking<VS, US, T, UT> {
        self.engine.get_hooks().trigger(OnInitHook {}).unwrap();
        EmbeddedBlocking { engine: self.engine }
    }
}

impl<VS, US, T, UT> WithHooks<VS, US, T, UT> for EmbeddedBlockingBuilder<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VS, US, T, UT> {
        &self.engine
    }
}
