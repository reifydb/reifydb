// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::ReifyDB;

fn main() {
    let instance = ReifyDB::in_memory();
    instance.tx("create schema test");
    instance.tx("create table test.users(id: int2, name: text, is_premium: bool)");

    instance.tx(r#"insert into test.users(id, name, is_premium) values (1,'Alice',true)"#);
    instance.tx(r#"insert into test.users(id, name, is_premium) values (2,'Bob', false)"#);
    instance.tx(r#"insert into test.users(id, name, is_premium) values (3,'Tina', false)"#);

    let result = instance.rx(r#"
        from test.users
        limit 3
        select id, is_premium
    "#);

    println!("{:?}", result);
}
