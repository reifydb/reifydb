// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::SystemBuilder;

use crate::hook::WithHooks;
use crate::session::{CommandSession, IntoCommandSession, IntoQuerySession, QuerySession, Session};
#[cfg(feature = "embedded_async")]
use crate::session::SessionAsync;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_system::ReifySystem;

/// System variant that provides unified lifecycle management for ReifyDB Engine and subsystems
///
/// This variant uses the ReifySystem architecture to coordinate the engine and multiple
/// subsystems (like FlowSubsystem, gRPC server, WebSocket server) in a single coherent system.
pub struct System<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) reify_system: ReifySystem<VT, UT>,
}

impl<VT, UT> System<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Create a new System with the given ReifySystem
    pub fn new(reify_system: ReifySystem<VT, UT>) -> Self {
        Self { reify_system }
    }

    /// Start the entire system (engine and all subsystems)
    pub fn start(&mut self) -> crate::Result<()> {
        self.reify_system.start().map_err(Into::into)
    }

    /// Stop the entire system gracefully
    pub fn stop(&mut self) -> crate::Result<()> {
        self.reify_system.stop().map_err(Into::into)
    }

    /// Check if the system is running
    pub fn is_running(&self) -> bool {
        self.reify_system.is_running()
    }

    // /// Get the current health status of the system
    // pub fn health_status(&self) -> reifydb_system::HealthStatus {
    //     self.reify_system.health_status()
    // }
    //
    // /// Get health status of all components
    // pub fn get_all_component_health(&self) -> std::collections::HashMap<String, reifydb_system::health::ComponentHealth> {
    //     self.reify_system.get_all_component_health()
    // }
    //
    // /// Get the names of all managed subsystems
    // pub fn get_subsystem_names(&self) -> Vec<String> {
    //     self.reify_system.get_subsystem_names()
    // }
    //
    // /// Update health monitoring for all components
    // pub fn update_health_monitoring(&mut self) {
    //     self.reify_system.update_health_monitoring()
    // }
    //
    // /// Get the number of managed subsystems
    // pub fn subsystem_count(&self) -> usize {
    //     self.reify_system.subsystem_count()
    // }
}

impl<VT, UT> Session<VT, UT> for System<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn command_session(
        &self,
        session: impl IntoCommandSession<VT, UT>,
    ) -> crate::Result<CommandSession<VT, UT>> {
        session.into_command_session(self.reify_system.engine().clone())
    }

    fn query_session(
        &self,
        session: impl IntoQuerySession<VT, UT>,
    ) -> crate::Result<QuerySession<VT, UT>> {
        session.into_query_session(self.reify_system.engine().clone())
    }
}

#[cfg(feature = "embedded_async")]
impl<VT, UT> SessionAsync<VT, UT> for System<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
}

impl<VT, UT> WithHooks<VT, UT> for System<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &reifydb_engine::Engine<VT, UT> {
        self.reify_system.engine()
    }
}