// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{DB, ReifyDB};

fn main() {
    let (db, root) = ReifyDB::embedded_blocking();
    // returns (db, root)
    // let session = db.session(root)
    // session.tx_execute('')
    db.tx_execute(&root, r#"create schema test"#);

    // let session = db.session(root.clone()).unwrap();
    // for result in session.execute("select 2, 3, 4") {
    //     println!("{}", result);
    // }
    //
    // let session = db.session_read_only(root.clone()).unwrap();
    // for result in session.execute("select 5, 6, 7, 8") {
    //     println!("{}", result);
    // }

    db.tx_execute(&root, r#"create table test.arith(id: int2, num: int2)"#);
    db.tx_execute(&root, r#"insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)"#);

    // for l in db.rx_execute(&root, r#"SELECT 1, 2 ,3 "#) {
    for l in db.rx_execute(&root, r#"FROM test.arith SELECT id, id + 1000, num"#) {
        println!("{}", l);
    }
    
    //
    // let result = db
    //     .rx_execute(&root, r#"from test.arith select id + 1, 2 + num + 3, id + num, num + num"#);
    //
    // for mut result in result {
    //     println!("{}", result);
    // }
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
//         let result = client.rx_execute(&query).await;
//
//         for l in &result {
//             print!("{}", l.to_string());
//         }
//     });
// }
