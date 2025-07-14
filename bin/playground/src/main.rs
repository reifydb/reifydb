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
    // db.tx_as(&root, r#"create table test.item(field_one: float8 policy (saturation undefined))"#).unwrap();
    // db.tx_as(&root, r#"create table test.item(field_one: uint8 policy (saturation error))"#).unwrap();
    db.tx_as(&root, r#"create table test.item(value: uint8)"#).unwrap();




    let err = db
        .tx_as(
            &root,
            r#"
          from [{value: 0}, {value: 1}, {value: 9999999999}, {value: 999999999999}, {value: 18446744073709551615}] insert test.item
        "#,
        )
        .unwrap_err();
    println!("{}", err);

    // let l = db
    //     .tx_as(
    //         &root,
    //         r#"
    //         from[{ field_one: 1.7e308 + 1e308}] insert test.item
    //     "#,
    //     )
    //     .unwrap();
    // println!("{}", l.first().unwrap());
    //

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
