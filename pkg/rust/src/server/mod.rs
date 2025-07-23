// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::hook::{HookContext, OnBeforeBootstrapHook, OnCreateHook};
use reifydb_core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use reifydb_network::grpc::server::{GrpcConfig, GrpcServer};
use reifydb_network::ws::server::{WsConfig, WsServer};
use std::error::Error;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinSet;

pub struct Server<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub(crate) engine: Engine<VS, US, T>,
    pub(crate) grpc_config: Option<GrpcConfig>,
    pub(crate) grpc: Option<GrpcServer<VS, US, T>>,
    pub(crate) ws_config: Option<WsConfig>,
    pub(crate) ws: Option<WsServer<VS, US, T>>,
}

impl<VS, US, T> Server<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn with_engine(mut self, engine: Engine<VS, US, T>) -> Self {
        self.engine = engine;
        self
    }

    pub fn with_grpc(mut self, config: GrpcConfig) -> Self {
        self.grpc_config = Some(config);
        self
    }

    pub fn with_websocket(mut self, config: WsConfig) -> Self {
        self.ws_config = Some(config);
        self
    }
}

impl<VS, US, T> Server<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(transaction: T) -> Self {
        Self {
            engine: Engine::new(transaction).unwrap(),

            grpc_config: None,
            grpc: None,

            ws_config: None,
            ws: None,
        }
    }

    pub fn ws_socket_addr(&self) -> Option<SocketAddr> {
        self.ws.as_ref().and_then(|ws| ws.socket_addr())
    }

    pub fn grpc_socket_addr(&self) -> Option<SocketAddr> {
        self.grpc.as_ref().and_then(|grpc| grpc.socket_addr())
    }

    pub fn before_bootstrap_hook<H>(self, hook: H) -> Self
    where
        H: OnBeforeBootstrapHook<VS, US, T>,
    {
        self.engine.hooks().lifecycle().before_bootstrap().register(Arc::new(hook));
        self
    }

    /// will only be invoked when a new database gets created
    pub fn on_create_hook<H>(self, hook: H) -> Self
    where
        H: OnCreateHook<VS, US, T>,
    {
        self.engine.hooks().lifecycle().on_create().register(Arc::new(hook));
        self
    }

    pub fn serve(&mut self, rt: &Runtime) -> Result<(), Box<dyn Error>> {
        // let before_ctx = OnBeforeBootstrapHookContext {};
        // self.engine
        //     .hooks()
        //     .lifecycle()
        //     .before_bootstrap()
        //     .for_each(|hook| hook.on_before_bootstrap(&before_ctx))?;
        //
        // let create_ctx = OnCreateHookContext { db: &self.engine };
        // self.engine.hooks().lifecycle().on_create().for_each(|hook| hook.on_create(&create_ctx))?;
        todo!();

        if let Some(config) = self.ws_config.take() {
            let engine = self.engine.clone();
            let ws = WsServer::new(config, engine);
            self.ws = Some(ws.clone());
            rt.spawn(async move { ws.serve().await.unwrap() });
        };

        if let Some(config) = self.grpc_config.take() {
            let engine = self.engine.clone();
            let grpc = GrpcServer::new(config, engine);
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
        // let before_ctx = OnBeforeBootstrapHookContext {};
        // self.engine
        //     .hooks()
        //     .lifecycle()
        //     .before_bootstrap()
        //     .for_each(|hook| hook.on_before_bootstrap(&before_ctx))
        //     .map_err(|e| {
        //         reifydb_core::error!(reifydb_core::error::diagnostic::engine::frame_error(
        //             e.to_string()
        //         ))
        //     })?;
        //
        // let create_ctx = OnCreateHookContext { db: &self.engine };
        // self.engine
        //     .hooks()
        //     .lifecycle()
        //     .on_create()
        //     .for_each(|hook| hook.on_create(&create_ctx))
        //     .map_err(|e| {
        //         reifydb_core::error!(reifydb_core::error::diagnostic::engine::frame_error(
        //             e.to_string()
        //         ))
        //     })?;

        todo!();

        rt.block_on(async {
            let mut handles = JoinSet::new();

            if let Some(config) = self.ws_config.take() {
                let engine = self.engine.clone();
                let ws = WsServer::new(config, engine);
                self.ws = Some(ws.clone());
                handles.spawn(
                    async move { ws.serve().await.map_err(|e| format!("WebSocket: {}", e)) },
                );
            };

            if let Some(config) = self.grpc_config.take() {
                let engine = self.engine.clone();
                let grpc = GrpcServer::new(config, engine);
                self.grpc = Some(grpc.clone());
                handles
                    .spawn(async move { grpc.serve().await.map_err(|e| format!("gRPC: {}", e)) });
            }

            loop {
                select! {
                    _ = signal => {
                        println!("shutting down");
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

struct HookClosure<VS, US, T, F>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    F: Fn(&HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
{
    f: F,
    _maker: PhantomData<(VS, US, T)>,
}

impl<VS, US, T, F> OnCreateHook<VS, US, T> for HookClosure<VS, US, T, F>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    F: Fn(&HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
{
    fn on_create(&self, ctx: &HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> {
        (self.f)(ctx)
    }
}

impl<VS, US, T> Server<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn before_bootstrap<F>(self, f: F) -> Self
    where
        F: Fn(&HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
    {
        // self.before_bootstrap(HookClosure { f, _maker: PhantomData::default() })
        todo!()
    }

    pub fn on_create<F>(self, f: F) -> Self
    where
        F: Fn(&HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
    {
        // self.on_create(HookClosure { f, _maker: PhantomData::default() })
        todo!()
    }
}
