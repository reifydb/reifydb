// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::storage::sqlite::SqliteConfig;
use reifydb::{ReifyDB, serializable, sqlite};

fn main() {
    let db = ReifyDB::embedded_blocking_with(serializable(sqlite(SqliteConfig::safe("/tmp"))));

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

    let l = db
        .tx_as_root(
            r#"
          from test.one
            natural left join { with test.two }
            natural left join { with test.three }
        "#,
        )
        .unwrap();

    // Debug: print the column names to see what they actually are
    let frame = l.first().unwrap();
    println!("Column names:");
    for (i, col) in frame.columns.iter().enumerate() {
        println!(
            "  [{}] name: '{}', qualified_name: '{}', frame: {:?}",
            i,
            col.name(),
            col.qualified_name(),
            col.frame()
        );
    }
    println!("Frame content:");
    println!("{}", frame);

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
