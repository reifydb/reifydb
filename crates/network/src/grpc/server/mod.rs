// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::grpc::server::db::DbService;
use crate::grpc::server::grpc::db_server::DbServer;
use reifydb_core::Error;
use reifydb_core::interface::{UnversionedTransaction, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use std::net::IpAddr::V4;
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Deref;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::OnceCell;
use tonic::service::InterceptorLayer;

pub mod auth;
mod db;

pub(crate) mod grpc {
    tonic::include_proto!("reifydb");
}

const DEFAULT_SOCKET: SocketAddr = SocketAddr::new(V4(Ipv4Addr::new(0, 0, 0, 0)), 54321);

#[derive(Debug)]
pub struct GrpcConfig {
    pub socket: Option<SocketAddr>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self { socket: Some(DEFAULT_SOCKET) }
    }
}

#[derive(Clone)]
pub struct GrpcServer<VS, US, T, UT>(Arc<Inner<VS, US, T, UT>>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction;

pub struct Inner<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    config: GrpcConfig,
    engine: Engine<VS, US, T, UT>,
    socket_addr: OnceCell<SocketAddr>,
    _phantom: std::marker::PhantomData<(VS, US, T, UT)>,
}

impl<VS, US, T, UT> Deref for GrpcServer<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    type Target = Inner<VS, US, T, UT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<VS, US, T, UT> GrpcServer<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn new(config: GrpcConfig, engine: Engine<VS, US, T, UT>) -> Self {
        Self(Arc::new(Inner {
            config,
            engine,
            socket_addr: OnceCell::new(),
            _phantom: std::marker::PhantomData,
        }))
    }

    pub async fn serve(self) -> Result<(), Error> {
        let listener =
            TcpListener::bind(self.config.socket.unwrap_or(DEFAULT_SOCKET)).await.unwrap();

        self.socket_addr.set(listener.local_addr().unwrap()).unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        tonic::transport::Server::builder()
            .layer(InterceptorLayer::new(auth::AuthInterceptor {}))
            .add_service(db_service(self.engine.clone()))
            .serve_with_incoming(incoming)
            .await
            .unwrap();

        Ok(())
    }

    pub fn socket_addr(&self) -> Option<SocketAddr> {
        self.socket_addr.get().cloned()
    }
}

// FIXME return result
pub fn db_service<VS, US, T, UT>(engine: Engine<VS, US, T, UT>) -> DbServer<DbService<VS, US, T, UT>>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    DbServer::new(DbService::new(engine))
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    // pub user_id: String,
    // pub roles: Vec<String>,
    // add more fields like email, tenant_id, etc.
}
