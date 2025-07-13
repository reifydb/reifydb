// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use reifydb_engine::frame::Frame;
use reifydb_network::grpc::server::{GrpcConfig, GrpcServer};
use reifydb_network::ws::server::{WsConfig, WsServer};
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinSet;
use tonic::transport::Error;

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
    pub(crate) callbacks: Callbacks<VS, US, T>,
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

pub type Callback<T> = Box<dyn Fn(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct Callbacks<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    before_bootstrap: Vec<Callback<BeforeBootstrap>>,
    on_create: Vec<Callback<OnCreate<VS, US, T>>>,
}

#[derive(Clone)]
pub struct Context(Arc<ContextInner>);

impl Deref for Context {
    type Target = ContextInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Context {
    pub fn info(&self, str: &str) {
        println!("info: {}", str);
    }
}

pub struct ContextInner {}

pub struct BeforeBootstrap {
    ctx: Context,
}

impl Deref for BeforeBootstrap {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

pub struct OnCreate<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    engine: Engine<VS, US, T>,
}

impl<VS, US, T> OnCreate<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn tx(&self, rql: &str) -> Vec<Frame> {
        self.engine.tx_as(&Principal::System { id: 1, name: "root".to_string() }, rql).unwrap()
    }

    pub fn rx(&self, rql: &str) -> Vec<Frame> {
        self.engine.rx_as(&Principal::System { id: 1, name: "root".to_string() }, rql).unwrap()
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
            callbacks: Callbacks { before_bootstrap: vec![], on_create: vec![] },
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

    pub fn before_bootstrap<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(BeforeBootstrap) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.before_bootstrap.push(Box::new(move |ctx| Box::pin(func(ctx))));
        self
    }

    /// will only be invoked when a new database gets created
    pub fn on_create<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(OnCreate<VS, US, T>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_create.push(Box::new(move |ctx| Box::pin(func(ctx))));
        self
    }

    pub fn serve(&mut self, rt: &Runtime) -> Result<(), Error> {
        let ctx = Context(Arc::new(ContextInner {}));

        for f in &self.callbacks.before_bootstrap {
            rt.block_on(async { f(BeforeBootstrap { ctx: ctx.clone() }).await });
        }

        for f in &self.callbacks.on_create {
            rt.block_on(async { f(OnCreate { engine: self.engine.clone() }).await });
        }

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
        rt.block_on(async {
            let ctx = Context(Arc::new(ContextInner {}));

            for f in &self.callbacks.before_bootstrap {
                f(BeforeBootstrap { ctx: ctx.clone() }).await;
            }

            for f in &self.callbacks.on_create {
                f(OnCreate { engine: self.engine.clone() }).await;
            }

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
