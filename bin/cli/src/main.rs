// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::{RDB, ReifyDB};

fn main() {
    let rdb = ReifyDB::embedded();
    rdb.tx("create schema test");
    rdb.tx("create table test.users(id: int2, name: text, is_premium: bool)");
    rdb.tx("create table test.projects(id: int2, name: text)");

    rdb.tx(r#"insert into test.users(id, is_premium, name) values (1,true,'Alice')"#);
    rdb.tx(r#"insert into test.users(id, name, is_premium) values (2,'Bob', false)"#);
    rdb.tx(r#"insert into test.users(id, name, is_premium) values (3,'Tina', true)"#);

    rdb.tx(r#"insert into test.projects(id, name) values (1,'A')"#);
    rdb.tx(r#"insert into test.projects(id, name) values (2,'B')"#);
    rdb.tx(r#"insert into test.projects(id, name) values (3,'C')"#);
    rdb.tx(r#"insert into test.projects(id, name) values (4,'D')"#);

    let result = rdb.rx(r#"
        from test.users
        limit 3
        select id, is_premium, id
    "#);

    println!("{:#?}", result);

    let result = rdb.rx(r#"
        from test.projects
        select id, name
    "#);

    println!("{:#?}", result);
}
