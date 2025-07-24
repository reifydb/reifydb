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
    db.tx_as_root(r#"create table test.item(field: int1)"#).unwrap();
    db.tx_as_root(r#"from [{field: -1 -2}] insert test.item"#).unwrap();
//     db.tx_as_root(
//         r#"
// from test.item map field
//     "#,
//     )
//     .unwrap();

    let l = db
        .tx_as_root(
            r#"
          from test.item map field
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
