// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::db_service;
pub use config::{DatabaseConfig, ServerConfig};
use reifydb_auth::Principal;
use reifydb_core::interface::Storage;
use reifydb_engine::{Engine, ExecutionResult};
use reifydb_transaction::Transaction;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tonic::service::InterceptorLayer;
use tonic::transport::Error;

mod config;
mod grpc;

pub struct Server<S: Storage, T: Transaction<S, S>> {
    pub(crate) config: ServerConfig,
    pub(crate) _grpc: tonic::transport::Server,
    pub(crate) callbacks: Callbacks<S, T>,
    pub(crate) engine: Engine<S, S, T>,
}

impl<S: Storage, T: Transaction<S, S>> Server<S, T> {
    pub fn with_config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_engine(mut self, engine: Engine<S, S, T>) -> Self {
        self.engine = engine;
        self
    }
}

pub type Callback<T> = Box<dyn FnOnce(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct Callbacks<S: Storage, T: Transaction<S, S>> {
    before_bootstrap: Vec<Callback<BeforeBootstrap>>,
    on_create: Vec<Callback<OnCreate<S, T>>>,
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

pub struct OnCreate<S: Storage, T: Transaction<S, S>> {
    engine: Engine<S, S, T>,
}

impl<S: Storage, T: Transaction<S, S>> OnCreate<S, T> {
    pub fn tx(&self, rql: &str) -> Vec<ExecutionResult> {
        self.engine.tx_as(&Principal::System { id: 1, name: "root".to_string() }, rql).unwrap()
    }

    pub fn rx(&self, rql: &str) -> Vec<ExecutionResult> {
        self.engine.rx_as(&Principal::System { id: 1, name: "root".to_string() }, rql).unwrap()
    }
}

impl<S: Storage + 'static, T: Transaction<S, S> + 'static> Server<S, T> {
    pub fn new(transaction: T) -> Self {
        Self {
            config: ServerConfig::default(),
            _grpc: tonic::transport::Server::builder(),
            callbacks: Callbacks { before_bootstrap: vec![], on_create: vec![] },
            engine: Engine::new(transaction).unwrap(),
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
        F: FnOnce(OnCreate<S, T>) -> Fut + Send + 'static,
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
            self.config.database.socket_addr.unwrap_or_else(|| "[::1]:54321".parse().unwrap());

        tonic::transport::Server::builder()
            .layer(InterceptorLayer::new(grpc::auth::AuthInterceptor {}))
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
                self.config.database.socket_addr.unwrap_or_else(|| "[::1]:54321".parse().unwrap());

            tonic::transport::Server::builder()
                .layer(InterceptorLayer::new(grpc::auth::AuthInterceptor {}))
                .add_service(db_service(self.engine))
                .serve(address)
                .await
                .unwrap();
        })
    }
}
