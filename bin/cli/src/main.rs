// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{ReifyDB, memory, optimistic};

fn main() {
    // let (db, root) = ReifyDB::embedded_blocking_with(optimistic(lmdb(&Path::new("/tmp/db"))));
    let (db, root) = ReifyDB::embedded_blocking_with(optimistic(memory()));
    // ReifyDB::embedded_blocking_with::<Memory, Serializable>(serializable());
    db.tx_as(&root, r#"create schema test"#);
    db.tx_as(&root, r#"create table test.users(id: int2, name: text, is_premium: bool)"#);
    db.tx_as(&root, r#"insert (1,true,'Alice') into test.users(id,is_premium, name)"#);
    // db.tx_as(&root, r#"insert (1,'Alice',true) into test.users(id,name, is_premium)"#);

    // let start = Instant::now();
    // for l in db.rx_as(&root, r#"from test.test"#) {
    for l in db.rx_as(&root, r#"from test.users limit 1 select id, name, name, is_premium, id"#) {
        println!("{}", l);
    }
    // println!("took {:?}", start.elapsed());
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
