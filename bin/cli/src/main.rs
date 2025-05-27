// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{DB, ReifyDB, lmdb, svl, mvcc};
use std::path::Path;
use reifydb::time::Instant;

fn main() {
    let (db, root) = ReifyDB::embedded_blocking_with(mvcc(lmdb(&Path::new("/tmp/db"))));
    db.tx_as(&root, r#"create schema test"#);
    db.tx_as(&root, r#"create series test.test(timestamp: int2, value: int2)"#);
    db.tx_as(
        &root,
        r#"
        insert
            (1,1),
            (2,2),
            (3,3),
            (4,4),
            (5,5),
            (6,6),
            (7,7),
            (8,8)
        into test.test(timestamp, value)"#,
    );

    let start = Instant::now();
    for l in db.rx_as(&root, r#"from test.test"#) {
    // for l in db.rx_as(&root, r#"from test.test group by timestamp select timestamp, avg(value)"#) {
        println!("{}", l);
    }
    println!("took {:?}", start.elapsed());
}

// use reifydb::client::Client;
//
// use reifydb::runtime::Runtime;
// use std::env;
// use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
//
// fn main() {
//     let rt = Runtime::new().unwrap();
//
//     rt.block_on(async {
//         let query = env::args().nth(1).expect("Usage: program '<query>'");
//
//         let client = Client {
//             socket_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 54321)),
//         };
//
//         let result = client.rx(&query).await;
//
//         for l in &result {
//             print!("{}", l.to_string());
//         }
//     });
// }
