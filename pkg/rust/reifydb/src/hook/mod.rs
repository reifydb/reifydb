// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod lifecycle;

pub use lifecycle::*;

use reifydb_core::hook::lifecycle::OnCreateHook;
use reifydb_core::interface::{GetHooks, UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;

pub trait WithHooks<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT>;

    fn on_create<F>(self, f: F) -> Self
    where
        Self: Sized,
        F: Fn(&OnCreateContext<VT, UT>) -> crate::Result<()> + Send + Sync + 'static,
    {
        let callback = OnCreateCallback { callback: f, engine: self.engine().clone() };

        self.engine().get_hooks().register::<OnCreateHook, _>(callback);
        self
    }
}
