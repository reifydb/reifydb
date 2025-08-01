// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;

pub use builder::ServerBuilder;

use crate::hook::WithHooks;
use reifydb_core::interface::{UnversionedTransaction, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use reifydb_network::grpc::server::{GrpcConfig, GrpcServer};
use reifydb_network::ws::server::{WsConfig, WsServer};
use std::net::SocketAddr;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinSet;

pub struct Server<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub(crate) engine: Engine<VS, US, T, UT>,
    pub(crate) grpc_config: Option<GrpcConfig>,
    pub(crate) grpc: Option<GrpcServer<VS, US, T, UT>>,
    pub(crate) ws_config: Option<WsConfig>,
    pub(crate) ws: Option<WsServer<VS, US, T, UT>>,
}

impl<VS, US, T, UT> Server<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn with_engine(mut self, engine: Engine<VS, US, T, UT>) -> Self {
        self.engine = engine;
        self
    }
}

impl<VS, US, T, UT> Server<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn new(engine: Engine<VS, US, T, UT>) -> Self {
        Self { engine, grpc_config: None, grpc: None, ws_config: None, ws: None }
    }

    pub fn ws_socket_addr(&self) -> Option<SocketAddr> {
        self.ws.as_ref().and_then(|ws| ws.socket_addr())
    }

    pub fn grpc_socket_addr(&self) -> Option<SocketAddr> {
        self.grpc.as_ref().and_then(|grpc| grpc.socket_addr())
    }

    /// will only be invoked when a new database gets created
    // pub fn on_create_hook<H>(self, hook: OnCreateHook) -> Self {
    //     // self.engine.hooks().lifecycle().on_create().register(Arc::new(hook));
    //     todo!();
    //     self
    // }

    pub fn serve(&mut self, rt: &Runtime) -> crate::Result<()> {
        if let Some(config) = self.ws_config.take() {
            let engine = self.engine.clone();
            let ws = WsServer::new(config, engine.clone());
            self.ws = Some(ws.clone());
            rt.spawn(async move { ws.serve().await.unwrap() });
        };

        if let Some(config) = self.grpc_config.take() {
            let engine = self.engine.clone();
            let grpc = GrpcServer::new(config, engine.clone());
            self.grpc = Some(grpc.clone());
            rt.spawn(async move { grpc.serve().await });
        }

        Ok(())
    }

    pub fn serve_blocking(
        &mut self,
        rt: &Runtime,
        signal: Receiver<()>,
    ) -> Result<(), reifydb_core::Error> {
        rt.block_on(async {
            let mut handles = JoinSet::new();

            if let Some(config) = self.ws_config.take() {
                let engine = self.engine.clone();
                let ws = WsServer::new(config, engine.clone());
                self.ws = Some(ws.clone());
                handles.spawn(
                    async move { ws.serve().await.map_err(|e| format!("WebSocket: {}", e)) },
                );
            };

            if let Some(config) = self.grpc_config.take() {
                let engine = self.engine.clone();
                let grpc = GrpcServer::new(config, engine.clone());
                self.grpc = Some(grpc.clone());
                handles
                    .spawn(async move { grpc.serve().await.map_err(|e| format!("gRPC: {}", e)) });
            }

            loop {
                select! {
                    _ = signal => {
                        self.close().await;
                        break;
                    }
                    result = handles.join_next(), if !handles.is_empty() => {
                        match result {
                            Some(Ok(Ok(()))) => {
                                println!("A server completed successfully");
                                break;
                            }
                            Some(Ok(Err(e))) => {
                                eprintln!("Server error: {}", e);
                                break;
                            }
                            Some(Err(e)) => {
                                eprintln!("Server panicked: {}", e);
                                self.close().await;
                                break;
                            }
                            None => {
                                println!("All servers have stopped");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn close(&mut self) {
        if let Some(ws) = self.ws.as_mut() {
            ws.close().await.unwrap();
        }

        if let Some(_grpc) = self.grpc.as_mut() {
            // grpc.close();
        }
    }
}

impl<VS, US, T, UT> WithHooks<VS, US, T, UT> for Server<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    fn engine(&self) -> &Engine<VS, US, T, UT> {
        &self.engine
    }
}
