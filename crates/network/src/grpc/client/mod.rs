// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod convert;
mod rx;
mod tx;

use std::net::SocketAddr;

pub(crate) mod grpc {
    tonic::include_proto!("reifydb");
}

pub struct GrpcClient {
    pub socket_addr: SocketAddr,
}
