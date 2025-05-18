// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::grpc_query::query_server::QueryServer;
use crate::server::grpc::query::QueryService;

pub mod auth;
mod query;

pub(crate) mod grpc_query {
    tonic::include_proto!("grpc_query");
}

// FIXME return result
pub fn query_service() -> QueryServer<QueryService> {
    QueryServer::new(QueryService {})
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: String,
    pub roles: Vec<String>,
    // add more fields like email, tenant_id, etc.
}
