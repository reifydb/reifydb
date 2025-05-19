// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::net::{SocketAddr, TcpListener};

pub fn free_local_socket() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind to ephemeral port");
    let addr = listener.local_addr().expect("failed to get local addr");
    drop(listener);
    addr
}
