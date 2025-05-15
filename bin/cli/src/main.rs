// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::{DB, ReifyDB};

fn main() {
    let db = ReifyDB::embedded();
    db.tx_execute(r#"create schema test"#);
    db.tx_execute(r#"create table test.arith(id: int2, num: int2)"#);
    db.tx_execute(r#"insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)"#);

    let result =
        db.rx_execute(r#"from test.arith select id + 1, 2 + num + 3, id + num, num + num"#);
    for mut result in result {
        println!("{}", result);
    }
}
