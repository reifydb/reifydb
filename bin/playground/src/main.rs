// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{ReifyDB, memory, serializable};

// FIXME to test later
// map @2024-03-15T14:30:00.123456789Z as result;

fn main() {
    let (db, root) = ReifyDB::embedded_blocking_with(serializable(memory()));

    db.tx_as(&root, r#"create schema test"#).unwrap();
    db.tx_as(&root, r#"create table test.float4_test10(col: float4)"#).unwrap();
    db.tx_as(&root, r#"from [{ col: 128.0 }] insert test.float4_test10"#).unwrap();
    // db.tx_as(&root, r#"create table test.item(field_one: float8 policy (saturation undefined))"#).unwrap();
    // db.tx_as(&root, r#"create table test.item(field_one: uint8 policy (saturation error))"#).unwrap();
    // db.tx_as(&root, r#"create table test.item(field: int16 policy (saturation error) )"#).unwrap();
    // db.tx_as(&root, r#"create table test.text_test5(col: text)"#).unwrap();
    // db.tx_as(&root, r#"from [{ col: "yes" }] insert test.text_test5"#).unwrap();
    // db.tx_as(&root, r#"from test.text_test5 map cast(col, bool)"#).unwrap();
    // db.tx_as(&root, r#"(tx 'from [{ col: cast("P30D", interval)}] insert test.text_test9')"#).unwrap();

    //   let l = db
    //       .tx_as(
    //           &root,
    //           r#"
    // from [
    //   { col: @2025-07-15  },
    //   { col: @2023-11-23  },
    // ] insert test.abc
    //       "#,
    //       )
    //       .unwrap();
    //   println!("{}", l.first().unwrap());

    let l = db
        .tx_as(
            &root,
            r#"
    map cast(cast(9223372036854775807, int8), float4)
        "#,
        )
        .unwrap_err();
    println!("{}", l);

    // let err = db
    //     .tx_as(
    //         &root,
    //         r#"
    //       map cast(1, int1)
    //     "#,
    //     )
    //     .unwrap_err();
    // println!("{}", err);
    //
    // // Test simple filter without map
    // let l = db
    //     .tx_as(
    //         &root,
    //         r#"
    //         map cast(undefined, float8)
    //         "#,
    //     )
    //     .unwrap();
    // println!("{}", l.first().unwrap());
    //
    // // Test map without filter
    // let l3 = db
    //     .tx_as(
    //         &root,
    //         r#"from test.edge_cases map content"#,
    //     )
    //     .unwrap();
    // println!("Map test (content column):");
    // println!("{}", l3.first().unwrap());
}
