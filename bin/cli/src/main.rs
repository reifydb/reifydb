// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::engine::{Engine, TransactionMut};
use reifydb::{ReifyDB, Value};

fn main() {
    let instance = ReifyDB::in_memory();
    instance.tx("create schema test");
    instance.tx("create table test.users(id: int2, name: text, is_premium: bool)");

    let mut tx = instance.engine().begin().unwrap();
    tx.insert(
        "users",
        vec![
            vec![Value::Int2(1), Value::Text("Alice".to_string()), Value::Boolean(true)],
            vec![Value::Int2(2), Value::Text("Bob".to_string()), Value::Boolean(false)],
            vec![Value::Int2(3), Value::Text("Tina".to_string()), Value::Boolean(false)],
        ],
    )
    .unwrap();

    tx.commit().unwrap();

    let result = instance.rx(r#"
        from test.users
        limit 3
        select id, is_premium
    "#);

    println!("{:?}", result);
}
