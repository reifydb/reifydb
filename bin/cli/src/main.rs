// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::engine::execute::execute_plan;
use reifydb::engine::{Engine, TransactionMut};
use reifydb::rql::ast;
use reifydb::rql::plan::plan;
use reifydb::storage::Memory;
use reifydb::{Value, engine};

fn main() {
    let engine = engine::svl::Engine::new(Memory::default());

    let tx = engine.begin().unwrap();
    tx.set(
        "users",
        vec![
            vec![Value::Int2(1), Value::Text("Alice".to_string()), Value::Boolean(true)],
            vec![Value::Int2(2), Value::Text("Bob".to_string()), Value::Boolean(false)],
            vec![Value::Int2(3), Value::Text("Tina".to_string()), Value::Boolean(false)],
        ],
    )
    .unwrap();

    tx.commit().unwrap();

    let rx = engine.begin_read_only().unwrap();

    let statements = ast::parse(
        r#"
        from users
        limit 3
        select id, name, name
    "#,
    );

    for statement in statements {
        let plan = plan(statement).unwrap();

        let result = execute_plan(&plan, &rx).unwrap();
        for row in result {
            println!("{:?}", row);
        }
    }
}
