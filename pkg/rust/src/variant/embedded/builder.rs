// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::Embedded;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;

pub struct EmbeddedBuilder<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    engine: Engine<VS, US, T>,
}

impl<VS, US, T> EmbeddedBuilder<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(transaction: T, hooks: Hooks) -> Self {
        Self { engine: Engine::new(transaction, hooks).unwrap() }
    }

    pub fn build(self) -> Embedded<VS, US, T> {
        Embedded { engine: self.engine }
    }
}

impl<VS, US, T> WithHooks<VS, US, T> for EmbeddedBuilder<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn engine(&self) -> &Engine<VS, US, T> {
        &self.engine
    }
}
