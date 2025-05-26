// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::db_service;
use auth::Principal;
pub use config::{DatabaseConfig, ServerConfig};
use engine::Engine;
use engine::old_execute::ExecutionResult;
use persistence::Persistence;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tonic::service::InterceptorLayer;
use tonic::transport::Error;
use transaction::{Rx, Transaction, Tx};

mod config;
mod grpc;

pub struct Server<P: Persistence, T: Transaction<P>> {
    pub(crate) config: ServerConfig,
    pub(crate) grpc: tonic::transport::Server,
    pub(crate) callbacks: Callbacks<P, T>,
    pub(crate) engine: Engine<P, T>,
}

impl<P: Persistence, T: Transaction<P>> Server<P, T> {
    pub fn with_config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_engine(mut self, engine: Engine<P, T>) -> Self {
        self.engine = engine;
        self
    }
}

pub type Callback<T> = Box<dyn FnOnce(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct Callbacks<P: Persistence, T: Transaction<P>> {
    before_bootstrap: Vec<Callback<BeforeBootstrap>>,
    on_create: Vec<Callback<OnCreate<P, T>>>,
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

pub struct OnCreate<P: Persistence, T: Transaction<P>> {
    engine: Engine<P, T>,
}

impl<P: Persistence, T: Transaction<P>> OnCreate<P, T> {
    pub fn tx(&self, rql: &str) -> Vec<ExecutionResult> {
        self.engine.tx_as(&Principal::System { id: 1, name: "root".to_string() }, &rql).unwrap()
    }

    pub fn rx(&self, rql: &str) -> Vec<ExecutionResult> {
        self.engine.rx_as(&Principal::System { id: 1, name: "root".to_string() }, &rql).unwrap()
    }
}

impl<P: Persistence + 'static, T: Transaction<P> + 'static> Server<P, T> {
    pub fn new(transaction: T) -> Self {
        Self {
            config: ServerConfig::default(),
            grpc: tonic::transport::Server::builder(),
            callbacks: Callbacks { before_bootstrap: vec![], on_create: vec![] },
            engine: Engine::new(transaction),
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
        F: FnOnce(OnCreate<P, T>) -> Fut + Send + 'static,
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
