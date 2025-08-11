// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::System;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::hook::lifecycle::OnInitHook;
use reifydb_core::interface::{GetHooks, UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;
#[cfg(feature = "sub_grpc")]
use reifydb_network::grpc::server::GrpcConfig;
#[cfg(feature = "sub_ws")]
use reifydb_network::ws::server::WsConfig;
use reifydb_system::{ReifySystemBuilder, Subsystem, SyncContext, SystemContext, TokioContext};
use std::sync::Arc;
use std::time::Duration;

/// Builder for configuring and constructing a System
///
/// The SystemBuilder provides a fluent interface for configuring
/// the ReifyDB system with engine and various subsystems before starting it up.
/// Uses type-state pattern with context parameter for compile-time optimization.
pub struct SystemBuilder<VT, UT, Ctx = SyncContext>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    Ctx: SystemContext,
{
    engine: Engine<VT, UT>,
    reify_system_builder: ReifySystemBuilder<VT, UT>,
    context: Ctx,
}

impl<VT, UT> SystemBuilder<VT, UT, SyncContext>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Create a new SystemBuilder with the given engine components
    /// Starts with SyncContext (no async runtime) by default
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> Self {
        let engine = Engine::new(versioned, unversioned, hooks).unwrap();
        let reify_system_builder = ReifySystemBuilder::new(engine.clone());

        Self { engine, reify_system_builder, context: SyncContext::default() }
    }

    /// Transform to async context with default Tokio runtime
    /// This enables async subsystems like gRPC and WebSocket servers
    #[cfg(feature = "async")]
    pub fn with_async_runtime(self) -> SystemBuilder<VT, UT, TokioContext> {
        let context =
            TokioContext::default().expect("Failed to create Tokio runtime for async operations");

        SystemBuilder {
            engine: self.engine,
            reify_system_builder: self.reify_system_builder,
            context,
        }
    }

    /// Transform to async context with custom Tokio runtime configuration
    #[cfg(feature = "async")]
    pub fn with_tokio_runtime(
        self,
        builder: tokio::runtime::Builder,
    ) -> SystemBuilder<VT, UT, TokioContext> {
        let context = TokioContext::with_builder(builder)
            .expect("Failed to create Tokio runtime with custom configuration");

        SystemBuilder {
            engine: self.engine,
            reify_system_builder: self.reify_system_builder,
            context,
        }
    }

    /// Transform to async context with user-provided runtime
    #[cfg(feature = "async")]
    pub fn with_custom_runtime(
        self,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> SystemBuilder<VT, UT, TokioContext> {
        let context = TokioContext::from_runtime(runtime);

        SystemBuilder {
            engine: self.engine,
            reify_system_builder: self.reify_system_builder,
            context,
        }
    }
}

// Generic implementation for all context types
impl<VT, UT, Ctx> SystemBuilder<VT, UT, Ctx>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    Ctx: SystemContext,
{
    /// Configure for development (shorter timeouts, more frequent health checks)
    pub fn development_config(mut self) -> Self {
        self.reify_system_builder = self.reify_system_builder.development_config();
        self
    }

    /// Configure for production (longer timeouts, less frequent health checks)
    pub fn production_config(mut self) -> Self {
        self.reify_system_builder = self.reify_system_builder.production_config();
        self
    }

    /// Configure for testing (very short timeouts)
    pub fn testing_config(mut self) -> Self {
        self.reify_system_builder = self.reify_system_builder.testing_config();
        self
    }

    /// Set the graceful shutdown timeout
    pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.reify_system_builder =
            self.reify_system_builder.with_graceful_shutdown_timeout(timeout);
        self
    }

    /// Set the health check interval
    pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
        self.reify_system_builder = self.reify_system_builder.with_health_check_interval(interval);
        self
    }

    /// Set the maximum startup time
    pub fn with_max_startup_time(mut self, timeout: Duration) -> Self {
        self.reify_system_builder = self.reify_system_builder.with_max_startup_time(timeout);
        self
    }

    /// Add a custom subsystem
    pub fn add_subsystem(mut self, subsystem: Box<dyn Subsystem>) -> Self {
        self.reify_system_builder = self.reify_system_builder.add_subsystem(subsystem);
        self
    }

    /// Add a FlowSubsystem with the specified polling interval
    pub fn with_flow_subsystem(mut self, poll_interval: Duration) -> Self
    where
        VT::Query: reifydb_core::interface::CdcScan,
    {
        let flow_subsystem = self.engine.create_flow_subsystem(poll_interval);
        let adapter = reifydb_system::FlowSubsystemAdapter::new(flow_subsystem);
        self.reify_system_builder = self.reify_system_builder.add_subsystem(Box::new(adapter));
        self
    }

    /// Get access to the current system context
    pub fn context(&self) -> &Ctx {
        &self.context
    }

    /// Build the System
    pub fn build(self) -> System<VT, UT> {
        // Trigger initialization hooks
        self.engine.get_hooks().trigger(OnInitHook {}).unwrap();

        // Build the ReifySystem
        let reify_system = self.reify_system_builder.build();

        System::new(reify_system)
    }
}

// Implementation for TokioContext (after transformation from SyncContext)
impl<VT, UT> SystemBuilder<VT, UT, TokioContext>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Add a gRPC server subsystem (uses shared runtime)
    #[cfg(feature = "sub_grpc")]
    pub fn with_grpc_server(mut self, config: GrpcConfig) -> Self {
        let adapter = reifydb_system::GrpcSubsystemAdapter::new(
            config,
            self.engine.clone(),
            self.context.runtime(),
        );
        self.reify_system_builder = self.reify_system_builder.add_subsystem(Box::new(adapter));
        self
    }

    /// Add a WebSocket server subsystem (uses shared runtime)
    #[cfg(feature = "sub_ws")]
    pub fn with_websocket_server(mut self, config: WsConfig) -> Self {
        let adapter = reifydb_system::WsSubsystemAdapter::new(
            config,
            self.engine.clone(),
            self.context.runtime(),
        );
        self.reify_system_builder = self.reify_system_builder.add_subsystem(Box::new(adapter));
        self
    }
}

// Automatic async context transformation for SyncContext
impl<VT, UT> SystemBuilder<VT, UT, SyncContext>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Add a gRPC server subsystem - automatically transforms to async context
    #[cfg(all(feature = "sub_grpc", feature = "async"))]
    pub fn with_grpc_server(self, config: GrpcConfig) -> SystemBuilder<VT, UT, TokioContext> {
        self.with_async_runtime().with_grpc_server(config)
    }

    /// Add a WebSocket server subsystem - automatically transforms to async context
    #[cfg(all(feature = "sub_ws", feature = "async"))]
    pub fn with_websocket_server(self, config: WsConfig) -> SystemBuilder<VT, UT, TokioContext> {
        self.with_async_runtime().with_websocket_server(config)
    }
}

impl<VT, UT, Ctx> WithHooks<VT, UT> for SystemBuilder<VT, UT, Ctx>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    Ctx: SystemContext,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}
