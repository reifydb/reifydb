// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::Any;
use crate::health::HealthStatus;
use crate::subsystem::Subsystem;
use reifydb_core::Result;
use reifydb_core::interface::{CdcScan, UnversionedTransaction, VersionedTransaction};
use reifydb_engine::subsystem::flow::FlowSubsystem;

/// Adapter to make FlowSubsystem compatible with the Subsystem trait
///
/// This wrapper implements the Subsystem trait for FlowSubsystem, allowing
/// it to be managed by the Database architecture.
pub struct FlowSubsystemAdapter<VT: VersionedTransaction, UT: UnversionedTransaction> {
    /// The wrapped FlowSubsystem
    flow_subsystem: FlowSubsystem<VT, UT>,
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction> FlowSubsystemAdapter<VT, UT> {
    /// Create a new FlowSubsystem adapter
    pub fn new(flow_subsystem: FlowSubsystem<VT, UT>) -> Self {
        Self { flow_subsystem }
    }

    /// Get a reference to the underlying FlowSubsystem
    pub fn inner(&self) -> &FlowSubsystem<VT, UT> {
        &self.flow_subsystem
    }

    /// Get a mutable reference to the underlying FlowSubsystem
    pub fn inner_mut(&mut self) -> &mut FlowSubsystem<VT, UT> {
        &mut self.flow_subsystem
    }
}

impl<VT, UT> Subsystem for FlowSubsystemAdapter<VT, UT>
where
    VT: VersionedTransaction + Send + Sync,
    UT: UnversionedTransaction + Send + Sync,
{
    fn name(&self) -> &'static str {
        "Flow"
    }

    fn start(&mut self) -> Result<()> {
        self.flow_subsystem.start()
    }

    fn stop(&mut self) -> Result<()> {
        self.flow_subsystem.stop()
    }

    fn is_running(&self) -> bool {
        self.flow_subsystem.is_running()
    }

    fn health_status(&self) -> HealthStatus {
        if self.flow_subsystem.is_running() { HealthStatus::Healthy } else { HealthStatus::Unknown }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
