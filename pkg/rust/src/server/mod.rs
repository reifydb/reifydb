// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::query_service;
use crate::{DB, IntoSessionRx, IntoSessionTx};
pub use config::{DatabaseConfig, ServerConfig};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tonic::service::InterceptorLayer;
use tonic::transport::Error;

mod config;
mod grpc;

pub struct Server {
    pub(crate) config: ServerConfig,
    pub(crate) grpc: tonic::transport::Server,
    pub(crate) callback: Callback,
}

pub type CallbackType<T> = Box<dyn FnOnce(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct Callback {
    before_bootstrap: Vec<CallbackType<BeforeBootstrap>>,
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

impl Server {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            grpc: tonic::transport::Server::builder(),
            callback: Callback { before_bootstrap: vec![] },
        }
    }

    pub fn before_bootstrap<F, Fut>(mut self, func: F) -> Self
    where
        F: FnOnce(BeforeBootstrap) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callback.before_bootstrap.push(Box::new(move |ctx| Box::pin(func(ctx))));
        self
    }

    pub async fn serve(self) -> Result<(), Error> {
        let ctx = Context(Arc::new(ContextInner {}));

        for f in self.callback.before_bootstrap {
            f(BeforeBootstrap { ctx: ctx.clone() }).await;
        }

        let address =
            self.config.database.socket_addr.unwrap_or_else(|| "[::1]:4321".parse().unwrap());

        tonic::transport::Server::builder()
            .layer(InterceptorLayer::new(grpc::auth::AuthInterceptor {}))
            .add_service(query_service())
            .serve(address)
            .await
    }
}
