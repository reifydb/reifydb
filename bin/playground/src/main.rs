// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb::{memory, serializable, ReifyDB};

fn main() {
    let (db, root) = ReifyDB::embedded_blocking_with(serializable(memory()));

    for l in db.tx_as(
        &root,
        r#"
from [
 {field: 1},
 {field: 2},
 {field: 3},
]
        "#,
    )
    .unwrap()
    {
        println!("{}", l);
    }

}

