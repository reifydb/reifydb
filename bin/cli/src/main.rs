// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{ReifyDB, memory, optimistic};

fn main() {
    // let (db, root) = ReifyDB::embedded_blocking_with(optimistic(lmdb(&Path::new("/tmp/db"))));
    // let (db, root) = ReifyDB::embedded_blocking_with(optimistic(sqlite(&Path::new("/tmp/db/"))));
    let (db, root) = ReifyDB::embedded_blocking_with(optimistic(memory()));
    // ReifyDB::embedded_blocking_with::<Memory, Serializable>(serializable());
    db.tx_as(&root, r#"create schema test"#).unwrap();

    // db.tx_as(&root, r#"create table test.arith(id: int2, num: int2)"#).unwrap();
    // db.tx_as(&root, r#"insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)"#).unwrap();

    db.tx_as(&root, r#"create table test.item(field_one: int1 policy (saturation undefined), field_two: int2, field_three: int1)"#).unwrap();
    // if let Err(e) = db.tx_as(&root, r#"insert (-127 - 2, -255 - 255, -120 - 3) into test.item (field_one, field_two, field_three)"#) {
    if let Err(e) = db.tx_as(
        &root,
        r#"insert (1,2,3),(132,4,5),(2,6,7) into test.item (field_one, field_two, field_three)"#,
    ) {
        println!("{}", e);
    }

    // let start = Instant::now();
    for l in db.tx_as(&root, r#"from test.item select field_one, field_two, field_three"#).unwrap()
    {
        // for l in db.tx_as(&root, r#"from test.arith select id + 42, id + id + id"#).unwrap() {
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
