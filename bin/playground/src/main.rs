// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{ReifyDB, memory, serializable};

fn main() {
    let (db, root) = ReifyDB::embedded_blocking_with(serializable(memory()));

    db.tx_as(&root, r#"create schema test"#).unwrap();
    db.tx_as(&root, r#"create table test.products(id: int4, name: utf8, price: int4, status: utf8)"#).unwrap();
    db.tx_as(&root, r#"  from [
    { id: 1, name: "Widget", price: 100, status: "active" },
    { id: 2, name: "Gadget", price: 200, status: "inactive" },
    { id: 3, name: "Tool", price: 150, status: "active" },
    { id: 4, name: "Device", price: 300, status: "active" },
    { id: 5, name: "Component", price: 75, status: "inactive" }
  ] insert test.products"#).unwrap();

    // let l = db
    //     .tx_as(
    //         &root,
    //         r#"
    //       from test.abc
    //     "#,
    //     )
    //     .unwrap();
    // println!("{}", l.first().unwrap());

    println!("=== Just first filter ===");
    for l in db
        .tx_as(
            &root,
            r#"from test.products filter status == "active""#,
        )
        .unwrap()
    {
        println!("{}", l);
    }
    
    println!("\n=== Both filters ===");
    for l in db
        .tx_as(
            &root,
            r#"from test.products filter status == "active" filter price < 200"#,
        )
        .unwrap()
    {
        println!("{}", l);
    }
}
