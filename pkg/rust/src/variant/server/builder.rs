// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::Server;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::hook::lifecycle::OnInitHook;
use reifydb_core::interface::{GetHooks, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use reifydb_network::grpc::server::GrpcConfig;
use reifydb_network::ws::server::WsConfig;

pub struct ServerBuilder<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    engine: Engine<VS, US, T>,
    grpc_config: Option<GrpcConfig>,
    ws_config: Option<WsConfig>,
}

impl<VS, US, T> ServerBuilder<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(transaction: T, hooks: Hooks) -> Self {
        Self {
            engine: Engine::new(transaction, hooks).unwrap(),
            grpc_config: None,
            ws_config: None,
        }
    }

    pub fn with_grpc(mut self, config: GrpcConfig) -> Self {
        self.grpc_config = Some(config);
        self
    }

    pub fn with_websocket(mut self, config: WsConfig) -> Self {
        self.ws_config = Some(config);
        self
    }

    pub fn build(self) -> Server<VS, US, T> {
        self.engine.get_hooks().trigger(OnInitHook {}).unwrap();

        let mut server = Server::new(self.engine);
        server.grpc_config = self.grpc_config;
        server.ws_config = self.ws_config;
        server
    }
}

impl<VS, US, T> WithHooks<VS, US, T> for ServerBuilder<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn engine(&self) -> &Engine<VS, US, T> {
        &self.engine
    }
}
