// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::{ReifyDB, memory, serializable};

fn main() {
    let db = ReifyDB::embedded_blocking_with(serializable(memory()));

    db.tx_as_root(r#"create schema test"#).unwrap();
    db.tx_as_root(r#"create table test.one(field: int1, other: int1)"#).unwrap();
    db.tx_as_root(r#"create table test.two(field: int1, name: text)"#).unwrap();
    db.tx_as_root(r#"create table test.three(field: int1, type: text)"#).unwrap();
    db.tx_as_root(r#"from [{field: 1, other: 2}, {field: 2, other: 2}, {field: 3, other: 2}, {field: 4, other: 2}, {field: 5, other: 2}] insert test.one"#).unwrap();
    db.tx_as_root(
        r#"from [{field: 2, name: "Peter"}, {field: 5, name: "Parker"}] insert test.two"#,
    )
    .unwrap();
    db.tx_as_root(r#"from [{field: 5, type: "Barker"}] insert test.three"#).unwrap();

    for frame in db
        .tx_as_root(
            r#"
   map { not cast('550e8400-e29b-41d4-a716-446655440000', uuid4)}
        "#,
        )
        .unwrap()
    {
        println!("{}", frame);
    }
}
