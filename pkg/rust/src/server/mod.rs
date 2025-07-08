// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use reifydb_engine::frame::Frame;
use reifydb_network::grpc;
use reifydb_network::grpc::server::{GrpcConfig, db_service};
use reifydb_network::ws::server::{WsConfig, WsServer};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tonic::service::InterceptorLayer;
use tonic::transport::Error;

pub struct Server<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub(crate) engine: Engine<VS, US, T>,
    pub(crate) _grpc: tonic::transport::Server,
    pub(crate) grpc_config: Option<GrpcConfig>,
    // pub(crate) grpc: Option<GrpcServer<VS, US,T>>,
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
        self.engine.execute_as(&Principal::System { id: 1, name: "root".to_string() }, rql).unwrap()
    }

    pub fn rx(&self, rql: &str) -> Vec<Frame> {
        self.engine.query_as(&Principal::System { id: 1, name: "root".to_string() }, rql).unwrap()
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

            _grpc: tonic::transport::Server::builder(),
            grpc_config: None,

            ws: None,
            ws_config: None,
        }
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

    pub async fn serve(self) -> Result<(), Error> {
        let ctx = Context(Arc::new(ContextInner {}));

        for f in self.callbacks.before_bootstrap {
            f(BeforeBootstrap { ctx: ctx.clone() }).await;
        }

        for f in self.callbacks.on_create {
            f(OnCreate { engine: self.engine.clone() }).await;
        }

        let address =
            self.grpc_config.unwrap().socket.unwrap_or_else(|| "[::1]:54321".parse().unwrap());

        tonic::transport::Server::builder()
            .layer(InterceptorLayer::new(grpc::server::auth::AuthInterceptor {}))
            .add_service(db_service(self.engine))
            .serve(address)
            .await
    }

    pub fn serve_blocking(self, rt: Runtime) {
        rt.block_on(async {
            let ctx = Context(Arc::new(ContextInner {}));

            for f in self.callbacks.before_bootstrap {
                f(BeforeBootstrap { ctx: ctx.clone() }).await;
            }

            for f in self.callbacks.on_create {
                f(OnCreate { engine: self.engine.clone() }).await;
            }

            let address =
                self.grpc_config.unwrap().socket.unwrap_or_else(|| "[::1]:54321".parse().unwrap());

            tonic::transport::Server::builder()
                .layer(InterceptorLayer::new(grpc::server::auth::AuthInterceptor {}))
                .add_service(db_service(self.engine))
                .serve(address)
                .await
                .unwrap();
        })
    }

    pub fn serve_websocket(&mut self, rt: &Runtime) {
        let engine = self.engine.clone();
        let config = self.ws_config.take().unwrap();
        let mut ws_server = WsServer::new(config, engine);
        // let shutdown = ws_server.shutdown.clone();

        rt.spawn(async move {
            ws_server.serve().await.unwrap();
        });

        // self.ws_shutdown = Some(shutdown); // store shutdown handle if needed
    }

    pub fn close(&mut self) {
        if let Some(ws) = self.ws.as_mut() {
            ws.close();
        }
    }
}
