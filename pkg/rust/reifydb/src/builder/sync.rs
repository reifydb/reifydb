// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::DatabaseBuilder;
use crate::Database;
#[cfg(feature = "sub_flow")]
use crate::FlowSubsystem as Adapter;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;

/// Builder for synchronous database configurations
///
/// Provides a simplified API for creating sync-only databases
pub struct SyncBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    inner: DatabaseBuilder<VT, UT>,
    hooks: Hooks,
    engine: Engine<VT, UT>,
}

impl<VT, UT> SyncBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> Self {
        let engine = Engine::new(versioned, unversioned, hooks.clone()).unwrap();
        let mut inner = DatabaseBuilder::new(engine.clone());

        // Automatically add flow subsystem if feature is enabled
        #[cfg(feature = "sub_flow")]
        {
            use reifydb_engine::subsystem::flow::FlowSubsystem;
            use std::time::Duration;

            let flow = FlowSubsystem::new(engine.clone(), Duration::from_millis(100));
            let flow_subsystem = Box::new(Adapter::new(flow));
            inner = inner.add_subsystem(flow_subsystem);
        }

        Self { inner, hooks, engine }
    }

    pub fn build(self) -> Database<VT, UT> {
        self.inner.build()
    }
}
