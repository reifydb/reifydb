// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::ReifyDB;
use reifydb::network::grpc::server::GrpcConfig;
use reifydb::network::ws::server::WsConfig;
use reifydb::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();

    ReifyDB::server()
        .with_websocket(WsConfig::default())
        .with_grpc(GrpcConfig{ socket: "[::1]:54321".parse().ok() })
        .on_create(|ctx| async move {
            ctx.tx("create schema test");
            ctx.tx("create table test.arith(id: int2, value: int2, num: int2)");
            ctx.tx("insert (1,1,5), (1,1,10), (1,2,15), (2,1,10), (2,1,30) into test.arith(id,value,num)");
        })
        .serve_websocket( &rt);

    while true {}
}
