// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

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

pub type Callback<T> = Box<dyn FnOnce(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

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
        F: FnOnce(BeforeBootstrap) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.before_bootstrap.push(Box::new(move |ctx| Box::pin(func(ctx))));
        self
    }

    /// will only be invoked when a new database gets created
    pub fn on_create<F, Fut>(mut self, func: F) -> Self
    where
        F: FnOnce(OnCreate<VS, US, T>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_create.push(Box::new(move |ctx| Box::pin(func(ctx))));
        self
    }

    pub fn serve(&mut self, rt: &Runtime) -> Result<(), Error> {
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

    pub fn serve_blocking(&mut self, rt: &Runtime) -> Result<(), reifydb_core::Error> {
        rt.block_on(async {
            let mut handles = Vec::with_capacity(2);

            if let Some(config) = self.ws_config.take() {
                let engine = self.engine.clone();
                let ws = WsServer::new(config, engine);
                self.ws = Some(ws.clone());
                handles.push(rt.spawn(async move { ws.serve().await.unwrap() }));
            };

            if let Some(config) = self.grpc_config.take() {
                let engine = self.engine.clone();
                let grpc = GrpcServer::new(config, engine);
                self.grpc = Some(grpc.clone());
                handles.push(rt.spawn(async move { grpc.serve().await.unwrap() }));
            }

            for handle in handles {
                if let Err(err) = handle.await {
                    // FIXME
                    panic!("server exited with error: {}", err);
                }
            }
        });

        Ok(())
    }

    pub fn close(&mut self) {
        if let Some(ws) = self.ws.as_mut() {
            ws.close();
        }
    }
}
