// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Hook system for database lifecycle events
//! 
//! Provides a way to register callbacks that are executed during various
//! database lifecycle events like creation, startup, shutdown, etc.

mod lifecycle;

pub use lifecycle::*;

use reifydb_core::hook::lifecycle::OnCreateHook;
use reifydb_core::hook::{BoxedHookIter, Callback};
use reifydb_core::interface::{GetHooks, UnversionedTransaction, VersionedTransaction};
use reifydb_core::return_hooks;
use reifydb_engine::Engine;

/// Shared callback implementation for OnCreate hook
pub struct OnCreateCallback<VT, UT, F>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    F: Fn(&OnCreateContext<VT, UT>) -> crate::Result<()> + Send + Sync + 'static,
{
    pub callback: F,
    pub engine: Engine<VT, UT>,
}

impl<VT, UT, F> Callback<OnCreateHook> for OnCreateCallback<VT, UT, F>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    F: Fn(&OnCreateContext<VT, UT>) -> crate::Result<()> + Send + Sync + 'static,
{
    fn on(&self, _hook: &OnCreateHook) -> Result<BoxedHookIter, reifydb_core::Error> {
        let context = OnCreateContext::new(self.engine.clone());
        (self.callback)(&context)?;
        return_hooks!()
    }
}

/// Trait for types that can register lifecycle hooks
pub trait WithHooks<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Get access to the underlying engine
    fn engine(&self) -> &Engine<VT, UT>;

    /// Register an on_create hook that will be called during database creation
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