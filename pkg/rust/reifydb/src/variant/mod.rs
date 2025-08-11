// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder variants for different database configurations
//! 
//! This module provides specialized builder types that wrap DatabaseBuilder
//! to provide convenient APIs for different use cases:
//! - SyncBuilder: For synchronous database operations
//! - AsyncBuilder: For asynchronous database operations  
//! - ServerBuilder: For server deployments with network subsystems

use crate::{Database, DatabaseBuilder};
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
use crate::context::{RuntimeProvider, TokioRuntimeProvider};
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
            let flow_subsystem = Box::new(crate::subsystem::FlowSubsystemAdapter::new(flow));
            inner = inner.add_subsystem(flow_subsystem);
        }
        
        Self { inner, hooks, engine }
    }

    /// Build the database
    pub fn build(self) -> Database<VT, UT> {
        self.inner.build()
    }
}

impl<VT, UT> WithHooks<VT, UT> for SyncBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}

/// Builder for asynchronous database configurations
/// 
/// Provides a simplified API for creating async databases with runtime support
#[cfg(feature = "async")]
pub struct AsyncBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    inner: DatabaseBuilder<VT, UT>,
    hooks: Hooks,
    engine: Engine<VT, UT>,
}

#[cfg(feature = "async")]
impl<VT, UT> AsyncBuilder<VT, UT>
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
            let flow_subsystem = Box::new(crate::subsystem::FlowSubsystemAdapter::new(flow));
            inner = inner.add_subsystem(flow_subsystem);
        }
        
        Self { inner, hooks, engine }
    }

    /// Build the database
    pub fn build(self) -> Database<VT, UT> {
        self.inner.build()
    }
}

#[cfg(feature = "async")]
impl<VT, UT> WithHooks<VT, UT> for AsyncBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}

/// Builder for server database configurations
/// 
/// Provides APIs for configuring network subsystems like gRPC and WebSocket
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub struct ServerBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    inner: DatabaseBuilder<VT, UT>,
    hooks: Hooks,
    engine: Engine<VT, UT>,
    runtime_provider: RuntimeProvider,
}

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
impl<VT, UT> ServerBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> Self {
        let engine = Engine::new(versioned, unversioned, hooks.clone()).unwrap();
        let inner = DatabaseBuilder::new(engine.clone());
        let runtime_provider = RuntimeProvider::Tokio(
            TokioRuntimeProvider::new().expect("Failed to create Tokio runtime for server")
        );
        Self { inner, hooks, engine, runtime_provider }
    }

    /// Configure WebSocket server subsystem
    #[cfg(feature = "sub_ws")]
    pub fn with_websocket(mut self, config: reifydb_network::ws::server::WsConfig) -> Self {
        let subsystem = Box::new(crate::subsystem::WsSubsystemAdapter::new(
            config, 
            self.engine.clone(), 
            &self.runtime_provider
        ));
        self.inner = self.inner.add_subsystem(subsystem);
        self
    }

    /// Configure gRPC server subsystem  
    #[cfg(feature = "sub_grpc")]
    pub fn with_grpc(mut self, config: reifydb_network::grpc::server::GrpcConfig) -> Self {
        let subsystem = Box::new(crate::subsystem::GrpcSubsystemAdapter::new(
            config, 
            self.engine.clone(), 
            &self.runtime_provider
        ));
        self.inner = self.inner.add_subsystem(subsystem);
        self
    }

    /// Build the database
    pub fn build(self) -> Database<VT, UT> {
        self.inner.build()
    }
}

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
impl<VT, UT> WithHooks<VT, UT> for ServerBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}