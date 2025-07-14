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
    db.tx_as(&root, r#"create table test.edge_cases(id: int4, content: text)"#).unwrap();

    
    let l = db
        .tx_as(
            &root,
            r#"
  from [
    { id: 1, content: "" },
    { id: 2, content: "a" },
    { id: 3, content: "This is a very long string that contains multiple words and should test the storage and retrieval of longer UTF8 content in the database system to ensure it handles variable-length strings correctly." },
    { id: 4, content: "Line1\nLine2\nLine3" },
    { id: 5, content: "Tab\tSeparated\tValues" },
    { id: 6, content: "Mixed: English 中文 العربية ελληνικά 日本語 русский" }
  ] insert test.edge_cases
        "#,
        )
        .unwrap();


    println!("{}", l.first().unwrap());
        let l = db
        .tx_as(
            &root,
            r#"
            from test.edge_cases filter content == "This is a very long string that contains multiple words and should test the storage and retrieval of longer UTF8 content in the database system to ensure it handles variable-length strings correctly." map id
        "#,
        )
        .unwrap();
    println!("{}", l.first().unwrap());


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
