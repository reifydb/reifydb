// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::db::DbService;
use crate::server::grpc::grpc_db::db_server::DbServer;
use reifydb_core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;

pub mod auth;
mod db;

pub(crate) mod grpc_db {
    tonic::include_proto!("grpc_db");
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
