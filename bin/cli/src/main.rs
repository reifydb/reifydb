// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::engine::execute::{execute_plan_mut, execute_plan_query};
use reifydb::engine::{Engine, Transaction, TransactionMut};
use reifydb::rql::ast;
use reifydb::rql::plan::{Plan, plan};
use reifydb::schema::{Column, ColumnName, SchemaName, StoreName};
use reifydb::storage::Memory;
use reifydb::{Value, ValueType, engine};

fn main() {
    let engine = engine::svl::Engine::new(Memory::default());

    let mut tx = engine.begin().unwrap();

    let r = execute_plan_mut(
        Plan::CreateSchema { name: SchemaName::new("test"), if_not_exists: false },
        &mut tx,
    )
    .unwrap();

    dbg!(&r);

    let r = execute_plan_mut(
        Plan::CreateTable {
            schema: SchemaName::new("test"),
            name: StoreName::new("users"),
            if_not_exists: false,
            columns: vec![
                Column { name: ColumnName::new("id"), value_type: ValueType::Int2, default: None },
                Column {
                    name: ColumnName::new("name"),
                    value_type: ValueType::Text,
                    default: None,
                },
                Column {
                    name: ColumnName::new("gender"),
                    value_type: ValueType::Boolean,
                    default: None,
                },
            ],
        },
        &mut tx,
    );

    dbg!(&r);

    //
    //
    // tx.schema_mut("test")
    //     .unwrap()
    //     .create(Store {
    //         name: StoreName::new("users"),
    //         kind: StoreKind::Table(Table {
    //             name: TableName::new("users"),
    //             columns: vec![
    //                 Column {
    //                     name: ColumnName::new("id"),
    //                     value_type: ValueType::Int2,
    //                     default: None,
    //                 },
    //                 Column {
    //                     name: ColumnName::new("name"),
    //                     value_type: ValueType::Text,
    //                     default: None,
    //                 },
    //                 Column {
    //                     name: ColumnName::new("gender"),
    //                     value_type: ValueType::Boolean,
    //                     default: None,
    //                 },
    //             ],
    //         }),
    //     })
    //     .unwrap();

    tx.set(
        "users",
        vec![
            vec![Value::Int2(1), Value::Text("Alice".to_string()), Value::Boolean(true)],
            vec![Value::Int2(2), Value::Text("Bob".to_string()), Value::Boolean(false)],
            vec![Value::Int2(3), Value::Text("Tina".to_string()), Value::Boolean(false)],
        ],
    )
    .unwrap();

    // create schema test;
    // create table test.users(id: int2, name: text(255), gender: boolean);

    tx.commit().unwrap();

    let rx = engine.begin_read_only().unwrap();

    let statements = ast::parse(
        r#"
        from users
        limit 3
        select gender, id, name, name
    "#,
    );

    for statement in statements {
        let plan = plan(statement).unwrap();

        let result = execute_plan_query(&plan, &rx).unwrap();
        for row in result {
            println!("{:?}", row);
        }
    }
}
