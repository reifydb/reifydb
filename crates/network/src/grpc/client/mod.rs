// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod convert;
mod rx;
mod tx;

use reifydb_core::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};

pub(crate) mod grpc_db {
    tonic::include_proto!("grpc_db");
}

// FIXME 1ms is a little bit little for production - only for testing for now
async fn wait_for_socket(addr: &SocketAddr, timeout: Duration) -> Result<(), Error> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match TcpStream::connect(addr).await {
            Ok(_) => return Ok(()),
            Err(_) => sleep(Duration::from_millis(1)).await,
        }
    }

    // Err(Error::connection_error(format!("Timed out waiting for server to start at {}", addr)))
    panic!()
}

pub struct Client {
    pub socket_addr: SocketAddr,
}
