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
    db.tx_as(&root, r#"create table test.abc(id: int1, col: float4)"#).unwrap();
    db.tx_as(&root, r#"from [{ id: 1, col: 128.0 }] insert test.abc"#).unwrap();

    // let l = db
    //     .tx_as(
    //         &root,
    //         r#"
    //       from test.abc
    //     "#,
    //     )
    //     .unwrap();
    // println!("{}", l.first().unwrap());

    for l in db
        .tx_as(
            &root,
            r#"
            from test.abc
            map { id, col: '22222222' }
            update test.abc;

            from test.abc;
          "#,
        )
        .unwrap()
    {
        println!("{}", l);
    }
}
