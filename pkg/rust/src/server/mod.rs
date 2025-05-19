// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::query_service;
use crate::{DB, IntoSessionRx, IntoSessionTx};
pub use config::{DatabaseConfig, ServerConfig};
use engine::Engine;
use engine::execute::{ExecutionResult, execute_plan_mut};
use rql::ast;
use rql::plan::plan_mut;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use storage::StorageEngine;
use tonic::service::InterceptorLayer;
use tonic::transport::Error;
use transaction::{Rx, TransactionEngine, Tx};

mod config;
mod grpc;

pub struct Server<S: StorageEngine, T: TransactionEngine<S>> {
    pub(crate) config: ServerConfig,
    pub(crate) grpc: tonic::transport::Server,
    pub(crate) callbacks: Callbacks<S, T>,
    pub(crate) engine: Engine<S, T>,
}

impl<S: StorageEngine, T: TransactionEngine<S>> Server<S, T> {
    pub fn with_config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_engine(mut self, engine: Engine<S, T>) -> Self {
        self.engine = engine;
        self
    }
}

pub type Callback<T> = Box<dyn FnOnce(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct Callbacks<S: StorageEngine, T: TransactionEngine<S>> {
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

pub struct OnCreate<S: StorageEngine, T: TransactionEngine<S>> {
    engine: Engine<S, T>,
}

impl<S: StorageEngine, T: TransactionEngine<S>> OnCreate<S, T> {
    pub fn tx(&self, rql: &str) -> Vec<ExecutionResult> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let mut tx = self.engine.begin().unwrap();

        for statement in statements {
            let plan = plan_mut(tx.catalog().unwrap(), statement).unwrap();
            let er = execute_plan_mut(plan, &mut tx).unwrap();
            result.push(er);
        }

        tx.commit().unwrap();

        result
    }
}

impl<S: StorageEngine + 'static, T: TransactionEngine<S> + 'static> Server<S, T> {
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
            self.config.database.socket_addr.unwrap_or_else(|| "[::1]:4321".parse().unwrap());

        tonic::transport::Server::builder()
            .layer(InterceptorLayer::new(grpc::auth::AuthInterceptor {}))
            .add_service(query_service(self.engine))
            .serve(address)
            .await
    }
}
