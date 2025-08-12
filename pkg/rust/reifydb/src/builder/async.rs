// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::DatabaseBuilder;
use crate::Database;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::Transaction;
use reifydb_engine::Engine;

#[cfg(feature = "async")]
pub struct AsyncBuilder<T: Transaction>
{
    inner: DatabaseBuilder<T>,
    engine: Engine<T>,
}

#[cfg(feature = "async")]
impl<T: Transaction> AsyncBuilder<T>
{
    pub fn new(versioned: T::Versioned, unversioned: T::Unversioned, hooks: Hooks) -> Self {
        let engine = Engine::new(versioned, unversioned, hooks.clone()).unwrap();
        let inner = DatabaseBuilder::new(engine.clone());
        Self { inner, engine }
    }

    pub fn build(self) -> Database<T> {
        self.inner.build()
    }
}

#[cfg(feature = "async")]
impl<T: Transaction> WithHooks<T> for AsyncBuilder<T>
{
    fn engine(&self) -> &Engine<T> {
        &self.engine
    }
}
