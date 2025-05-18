// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::service::grpc_query::query_server::QueryServer;
use crate::server::service::query::QueryService;

mod query;

pub(crate) mod grpc_query {
    tonic::include_proto!("grpc_query");
}

// FIXME return result
pub fn query_service() -> QueryServer<QueryService> {
    QueryServer::new(QueryService {})
}
