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
    db.tx_as(&root, r#"create table test.abc(field: int1)"#).unwrap();
    db.tx_as(&root, r#"from [{ field: 1}] insert test.abc"#).unwrap();

    // let l = db
    //     .tx_as(
    //         &root,
    //         r#"
    //       from test.abc
    //     "#,
    //     )
    //     .unwrap();
    // println!("{}", l.first().unwrap());

    let err = db.tx_as(&root, r#"from test.abc map { field: 129} update test.abc"#).unwrap_err();
    println!("{}", err);

    // for l in db
    //     .tx_as(
    //         &root,
    //         r#"
    // from test.item map { field: 129} update test.item
    //       "#,
    //     )
    //     .unwrap()
    // {
    //     println!("{}", l);
    // }
}
