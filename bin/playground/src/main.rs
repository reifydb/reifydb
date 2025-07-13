// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{memory, serializable, ReifyDB};

fn main() {
    // let (db, root) = ReifyDB::embedded_blocking_with(optimistic(lmdb(&Path::new("/tmp/db"))));
    // let (db, root) = ReifyDB::embedded_blocking_with(optimistic(sqlite(&Path::new("/tmp/db/"))));
    let (db, root) = ReifyDB::embedded_blocking_with(serializable(memory()));
    db.tx_as(&root, r#"create schema test"#).unwrap();
    db.tx_as(&root, r#"create table test.one(field: int1, other: int1)"#).unwrap();
    db.tx_as(&root, r#"create table test.two(field: int1)"#).unwrap();
    let _err = db
        .tx_as(&root, r#"insert (1,2),(2,2),(3,2),(4,2),(5,2) into test.one (field, other)"#)
        .unwrap();
    let _err = db.tx_as(&root, r#"insert (2),(3) into test.two (field)"#).unwrap();
    // println!("{}", err);

    for l in db.tx_as(
        &root,
        r#"
from test.one
map 1 as a
        "#,
    )
    .unwrap()
    {
        println!("{}", l);
    }

    // for l in db.rx_as(
    //     &root,
    //     r#"
    //     from test.one left join test.two on one.field == two.field
    //     "#,
    // )
    // {
    //     println!("{}", l);
    // }


    // for l in db.rx_as(
    //     &root,
    //     r#"
    //     from test.one left join test.two on one.field == two.field
    //     "#,
    // )
    // // .unwrap()
    // {
    //     println!("{}", l);
    // }

    // db.tx_as(&root, r#"create table test.users(age: int2, num: float8)"#).unwrap();
    // db.tx_as(
    //     &root,
    //     r#"insert (3,1), (1,1), (1,3), (2,2), (2,4), (2,6) into test.users (age, num)"#,
    // )
    // .unwrap();
    // // db.tx_as(&root, r#"insert (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33) into test.another (age)"#).unwrap();
    // for l in db
    //     .tx_as(
    //         &root,
    //         r#"
    //         from test.users
    //         map age, num
    //         aggregate sum(num) as a, min(num) as b, max(num) as c by age
    //
    //         "#,
    //     )
    //     .unwrap()
    // {
    //     println!("{}", l);
    // }

    // for l in db.rx_as(
    //     &root,
    //     r#"
    //         map 1 + 1,2 + 2,3 * 12
    //     "#,
    // ) {
    //     println!("{}", l);
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
//         let result = client.rx(&query).await;
//
//         for l in &result {
//             print!("{}", l.to_string());
//         }
//     });
// }
