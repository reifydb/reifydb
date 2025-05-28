// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::db::DbService;
use crate::server::grpc::grpc_db::db_server::DbServer;
use persistence::Persistence;
use reifydb_engine::Engine;
use transaction::Transaction;

pub mod auth;
mod db;

pub(crate) mod grpc_db {
    tonic::include_proto!("grpc_db");
}

// FIXME return result
pub fn db_service<P: Persistence + 'static, T: Transaction<P> + 'static>(
    reifydb_engine: Engine<P, T>,
) -> DbServer<DbService<P, T>> {
    DbServer::new(DbService::new(reifydb_engine))
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: String,
    pub roles: Vec<String>,
    // add more fields like email, tenant_id, etc.
}
