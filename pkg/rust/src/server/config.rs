// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::net::SocketAddr;

#[derive(Debug, Default)]
pub struct ServerConfig {
    pub database: DatabaseConfig,
}

#[derive(Debug, Default)]
pub struct DatabaseConfig {
    pub socket_addr: Option<SocketAddr>,
}
