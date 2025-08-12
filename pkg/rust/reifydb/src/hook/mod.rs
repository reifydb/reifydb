// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod lifecycle;

pub use lifecycle::*;

use reifydb_core::hook::lifecycle::OnCreateHook;
use reifydb_core::interface::{GetHooks, Transaction};
use reifydb_engine::Engine;

pub trait WithHooks<T>
where
    T: Transaction,
{
    fn engine(&self) -> &Engine<T>;

    fn on_create<F>(self, f: F) -> Self
    where
        Self: Sized,
        F: Fn(&OnCreateContext<T>) -> crate::Result<()> + Send + Sync + 'static,
    {
        let callback = OnCreateCallback { callback: f, engine: self.engine().clone() };

        self.engine().get_hooks().register::<OnCreateHook, _>(callback);
        self
    }
}
