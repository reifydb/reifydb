// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{ReifyDB, memory, serializable};

fn main() {
    // let (db, root) = ReifyDB::embedded_blocking_with(optimistic(lmdb(&Path::new("/tmp/db"))));
    // let (db, root) = ReifyDB::embedded_blocking_with(optimistic(sqlite(&Path::new("/tmp/db/"))));
    let (db, root) = ReifyDB::embedded_blocking_with(serializable(memory()));
    db.tx_as(&root, r#"create schema test"#).unwrap();
    // if let Err(e) = db.tx_as(
    //     &root,
    //     r#"create table test.arith(id: int2, num: int2)"#,
    // ) {
    //     println!("{}", e);
    // }

    if let Err(e) = db.tx_as(&root, r#"create table test.item(field_one: uint4)"#) {
        println!("{}", e);
    }

    if let Err(e) = db.tx_as(&root, r#"insert (1),(2),(3) into test.item(field_one)"#) {
        println!("{}", e);
    }

    // for l in db.rx_as(
    //     &root,
    //     r#"
    //     from test.item
    //     filter field_one + 10 > 2
    //     select field_one, field_one + 1
    //     limit 1
    //     "#,
    // ) {
    //     println!("{}", l);
    // }

    // for l in db.rx_as(
    //     &root,
    //     r#"
    //     select 1, 'test', true, false
    //     "#,
    // ) {
    //     println!("{}", l);
    // }

    // for l in db.rx_as(
    //     &root,
    //     r#"
    //         select cast(127 as int1) > cast(200 as uint1)
    //     "#,
    // ) {
    //     println!("{}", l);
    // }

    for l in db.rx_as(
        &root,
        r#"
            select cast(32767 as int2) < cast(100000 as int16)
        "#,
    ) {
        println!("{}", l);
    }
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
