// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{ReifyDB, memory, serializable};

fn main() {
    let db = ReifyDB::embedded_blocking_with(serializable(memory()));

    db.tx_as_root(r#"create schema test"#).unwrap();
    db.tx_as_root(r#"create table test.nulls(id: int4, value: int4, name: utf8)"#).unwrap();
    db.tx_as_root(
        r#"
      from [
        { id: 1, value: 10, name: "valid" },
        { id: 2, value: undefined, name: "partial" },
        { id: 3, value: 20, name: undefined },
        { id: 4, value: undefined, name: undefined },
        { id: 5, value: 0, name: "zero" }
      ] insert test.nulls
    "#,
    )
    .unwrap();

    let l = db
        .tx_as_root(
            r#"
          map cast('550e8400-e29b-41d4-a716-44665544', UUID4) as uuid
        "#,
        )
        .unwrap();
    println!("{}", l.first().unwrap());

    //     for l in db
    //         .tx_as(
    //             &root,
    //             r#"from test.nulls filter value > 0
    // }"#,
    //         )
    //         .unwrap()
    //     {
    //         println!("{}", l);
    //     }
}
