// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::{DB, ReifyDB};

fn main() {
    let db = ReifyDB::embedded();
    db.tx_execute("create schema test");
    db.tx_execute("create table test.users(id: int2, name: text, is_premium: bool)");
    db.tx_execute("create table test.projects(id: int2, name: text)");

    db.tx_execute(r#"insert (1,true,'Alice') into test.users(id, is_premium, name) "#);
    db.tx_execute(r#"insert (2,'Bob', false) into test.users (id, name, is_premium)"#);
    db.tx_execute(r#"insert (3,'Tina', true) into test.users(id, name, is_premium)"#);

    db.tx_execute(r#"insert (1,'A') into test.projects(id, name)"#);
    db.tx_execute(r#"insert (2,'B') into test.projects(id, name)"#);
    db.tx_execute(r#"insert (3,'C') into test.projects(id, name)"#);
    db.tx_execute(r#"insert (4,'D') into test.projects(id, name)"#);

    let result = db.rx_execute(
        r#"
        from test.users
        limit 3
        select id, name, name, is_premium, id
    "#,
    );

    println!("{}", result[0]);

    // let result = db.rx_execute(
    //     r#"
    //     from test.projects
    //     select id, name
    // "#,
    // );

    let mut result = db.rx_execute("select 1, 'test', true, false");
    println!("{}", result.pop().unwrap());
}
