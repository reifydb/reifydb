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
use reifydb_core::interface::Transaction;
use reifydb_engine::Engine;
use reifydb_network::ws::server::WsConfig;

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub struct ServerBuilder<T>
where
    T: Transaction,
{
    inner: DatabaseBuilder<T>,
    engine: Engine<T>,
    runtime_provider: RuntimeProvider,
}

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
impl<T> ServerBuilder<T>
where
    T: Transaction,
{
    pub fn new(versioned: T::Versioned, unversioned: T::Unversioned, hooks: Hooks) -> Self {
        let engine = Engine::new(versioned, unversioned, hooks.clone()).unwrap();
        let inner = DatabaseBuilder::new(engine.clone());
        let runtime_provider = RuntimeProvider::Tokio(
            TokioRuntimeProvider::new().expect("Failed to create Tokio runtime for server"),
        );
        Self { inner, engine, runtime_provider }
    }

    #[cfg(feature = "sub_ws")]
    pub fn with_ws(mut self, config: WsConfig) -> Self {
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

    pub fn build(self) -> Database<T> {
        self.inner.build()
    }
}

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
impl<T> WithHooks<T> for ServerBuilder<T>
where
    T: Transaction,
{
    fn engine(&self) -> &Engine<T> {
        &self.engine
    }
}
