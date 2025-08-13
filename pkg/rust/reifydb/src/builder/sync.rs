// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::DatabaseBuilder;
use crate::Database;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::Transaction;
use reifydb_engine::Engine;

pub struct SyncBuilder<T: Transaction> {
    inner: DatabaseBuilder<T>,
}

impl<T: Transaction> SyncBuilder<T> {
    pub fn new(versioned: T::Versioned, unversioned: T::Unversioned, cdc: T::Cdc, hooks: Hooks) -> Self {
        Self {
            inner: DatabaseBuilder::new(
                Engine::new(versioned, unversioned, cdc, hooks.clone()).unwrap(),
            ),
        }
    }

    pub fn build(self) -> Database<T> {
        self.inner.build()
    }
}
