// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{ReifyDB, memory, serializable};

fn main() {
    let (db, root) = ReifyDB::embedded_blocking_with(serializable(memory()));

    // (tx 'create schema test')
    //     (tx 'create table test.item(field_one: int1, field_two: int1, field_three: int1)')
    //
    //      !tx 'from [{field_one: 127, field_two: 128, field_three: -128}] insert test.item'

    db.tx_as(&root, r#"create schema test"#).unwrap();
    db.tx_as(&root, r#"create table test.item(field_one: int1, field_two: int1, field_three: int1)"#).unwrap();
    let l = db
        .tx_as(
            &root,
            r#"
            from [{field_one: 127, field_two: 128, field_three: -128}] insert test.item
        "#,
        )
        .unwrap_err();
    println!("{}", l.to_string());


    // let l = db
    //     .tx_as(
    //         &root,
    //         r#"
    //         from [{x: 1}, {x: 2, y: 3}, {x: 4, y: 5, z: 6}]
    //     "#,
    //     )
    //     .unwrap();
    //
    // println!("{}", l.first().unwrap());

}
