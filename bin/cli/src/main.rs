// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![deny(clippy::unwrap_used)]
// #![deny(clippy::expect_used)]

// fn main() {
//     let (db, root) = ReifyDB::embedded();
//     // returns (db, root)
//     // let session = db.session(root)
//     // session.tx_execute('')
//     // db.tx_execute_as(&root, r#"create schema test"#);
//
//     let session = db.session(root.clone()).unwrap();
//     for result in session.execute("select 2, 3, 4") {
//         println!("{}", result);
//     }
//
//     let session = db.session_read_only(root.clone()).unwrap();
//     for result in session.execute("select 5, 6, 7, 8") {
//         println!("{}", result);
//     }
//
//     // db.tx_execute_as(&root, r#"create schema test"#);
//     // db.tx_execute_as(&root, r#"create table test.arith(id: int2, num: int2)"#);
//     // db.tx_execute_as(&root, r#"insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)"#);
//     //
//     // let result = db
//     //     .rx_execute_as(&root, r#"from test.arith select id + 1, 2 + num + 3, id + num, num + num"#);
//     //
//     // for mut result in result {
//     //     println!("{}", result);
//     // }
// }

use reifydb::client::Client;

use std::env;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let query = env::args().nth(1).expect("Usage: program '<query>'");

    let client = Client {
        socket_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4321)),
    };

    let result = client.tx_execute(&query).await;

    for l in &result {
        print!("{}", l.to_string());
    }
    Ok(())
}
