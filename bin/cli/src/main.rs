// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::{DB, ReifyDB};

fn main() {
    let db = ReifyDB::embedded();
    let result = db.rx_execute(r#"select 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8"#);
    for mut result in result {
        println!("{}", result);
    }
}
