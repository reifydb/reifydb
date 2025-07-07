// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::ReifyDB;
use reifydb::runtime::Runtime;
use reifydb::server::{DatabaseConfig, ServerConfig, WebsocketConfig};
use std::net::{SocketAddr, SocketAddrV4};
use std::str::FromStr;

fn main() {
    let rt = Runtime::new().unwrap();

    ReifyDB::server()
        .with_config(ServerConfig {
            database: DatabaseConfig { socket_addr: "127.0.0.1:54321".parse().ok() },
        })
        .on_create(|ctx| async move {
            ctx.tx("create schema test");
            ctx.tx("create table test.arith(id: int2, value: int2, num: int2)");
            ctx.tx("insert (1,1,5), (1,1,10), (1,2,15), (2,1,10), (2,1,30) into test.arith(id,value,num)");
        })
        .serve_websocket(WebsocketConfig{
            socket_addr: SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:9001").unwrap()),
          }, &rt);

    while true {

    }
}
