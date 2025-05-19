// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::db::DbService;
use crate::server::grpc::grpc_db::db_server::DbServer;
use engine::Engine;
use storage::StorageEngine;
use transaction::TransactionEngine;

pub mod auth;
mod db;

pub(crate) mod grpc_db {
    tonic::include_proto!("grpc_db");
}

// FIXME return result
pub fn query_service<S: StorageEngine + 'static, T: TransactionEngine<S> + 'static>(
    engine: Engine<S, T>,
) -> DbServer<DbService<S, T>> {
    DbServer::new(DbService { engine })
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: String,
    pub roles: Vec<String>,
    // add more fields like email, tenant_id, etc.
}
