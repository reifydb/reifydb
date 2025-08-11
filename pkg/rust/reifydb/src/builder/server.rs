// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::DatabaseBuilder;
use crate::Database;
#[cfg(feature = "sub_grpc")]
use crate::GrpcSubsystem;
#[cfg(feature = "sub_ws")]
use crate::WsSubsystem;
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
use crate::context::{RuntimeProvider, TokioRuntimeProvider};
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;
use reifydb_network::ws::server::WsConfig;

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub struct ServerBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    inner: DatabaseBuilder<VT, UT>,
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
            TokioRuntimeProvider::new().expect("Failed to create Tokio runtime for server"),
        );
        Self { inner, engine, runtime_provider }
    }

    #[cfg(feature = "sub_ws")]
    pub fn with_websocket(mut self, config: WsConfig) -> Self {
        let subsystem = WsSubsystem::new(config, self.engine.clone(), &self.runtime_provider);
        self.inner = self.inner.add_subsystem(subsystem);
        self
    }

    #[cfg(feature = "sub_grpc")]
    pub fn with_grpc(mut self, config: reifydb_network::grpc::server::GrpcConfig) -> Self {
        let subsystem = GrpcSubsystem::new(config, self.engine.clone(), &self.runtime_provider);
        self.inner = self.inner.add_subsystem(subsystem);
        self
    }

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
