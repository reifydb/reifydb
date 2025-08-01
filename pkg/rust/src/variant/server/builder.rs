// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::Server;
use crate::hook::WithHooks;
use reifydb_core::hook::Hooks;
use reifydb_core::hook::lifecycle::OnInitHook;
use reifydb_core::interface::{GetHooks, UnversionedTransaction, VersionedTransaction};
use reifydb_engine::Engine;
use reifydb_network::grpc::server::GrpcConfig;
use reifydb_network::ws::server::WsConfig;

pub struct ServerBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
    grpc_config: Option<GrpcConfig>,
    ws_config: Option<WsConfig>,
}

impl<VT, UT> ServerBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> Self {
        Self {
            engine: Engine::new(versioned, unversioned, hooks).unwrap(),
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

    pub fn build(self) -> Server<VT, UT> {
        self.engine.get_hooks().trigger(OnInitHook {}).unwrap();

        let mut server = Server::new(self.engine);
        server.grpc_config = self.grpc_config;
        server.ws_config = self.ws_config;
        server
    }
}

impl<VT, UT> WithHooks<VT, UT> for ServerBuilder<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VT, UT> {
        &self.engine
    }
}
