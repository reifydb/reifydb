// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::grpc::server::db::DbService;
use crate::grpc::server::grpc::db_server::DbServer;
use reifydb_core::Error;
use reifydb_core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use std::net::IpAddr::V4;
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Deref;
use std::sync::Arc;
use tonic::service::InterceptorLayer;

pub mod auth;
mod db;

pub(crate) mod grpc {
    tonic::include_proto!("reifydb");
}

const DEFAULT_SOCKET: SocketAddr = SocketAddr::new(V4(Ipv4Addr::new(127, 0, 0, 1)), 54321);

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
pub struct GrpcServer<VS, US, T>(Arc<Inner<VS, US, T>>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>;

pub struct Inner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    config: GrpcConfig,
    engine: Engine<VS, US, T>,
}

impl<VS, US, T> Deref for GrpcServer<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    type Target = Inner<VS, US, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<VS, US, T> GrpcServer<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(config: GrpcConfig, engine: Engine<VS, US, T>) -> Self {
        Self(Arc::new(Inner { config, engine }))
    }

    pub async fn serve(self) -> Result<(), Error> {
        tonic::transport::Server::builder()
            .layer(InterceptorLayer::new(crate::grpc::server::auth::AuthInterceptor {}))
            .add_service(db_service(self.engine.clone()))
            .serve(self.config.socket.unwrap_or(DEFAULT_SOCKET))
            .await
            .unwrap();

        Ok(())
    }
}

// FIXME return result
pub fn db_service<VS, US, T>(engine: Engine<VS, US, T>) -> DbServer<DbService<VS, US, T>>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    DbServer::new(DbService::new(engine))
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    // pub user_id: String,
    // pub roles: Vec<String>,
    // add more fields like email, tenant_id, etc.
}
