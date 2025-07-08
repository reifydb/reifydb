// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::grpc::server::db::DbService;
use crate::grpc::server::grpc::db_server::DbServer;
use reifydb_core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use std::net::SocketAddr;
use std::str::FromStr;

pub mod auth;
mod db;

pub(crate) mod grpc {
    tonic::include_proto!("reifydb");
}

#[derive(Debug)]
pub struct GrpcConfig {
    pub socket: Option<SocketAddr>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self { socket: Some(SocketAddr::from_str("[::1]:54321").unwrap()) }
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
