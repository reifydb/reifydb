// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::catalog::{
    Catalog, Column, ColumnName, Columns, Store, StoreKind, StoreName, TableName,
};
use reifydb::rql::ast;
use reifydb::rql::execute::execute_plan;
use reifydb::rql::plan::plan;
use reifydb::{Database, Table, Value, ValueType};
use std::collections::HashMap;

struct DummyCatalog {}

impl Catalog for DummyCatalog {
    fn get(&self, name: impl AsRef<str>) -> reifydb::catalog::Result<Option<Store>> {
        let name = name.as_ref();

        if name == "users" {
            Ok(Some(Store {
                name: StoreName::new("users"),
                kind: StoreKind::Table(reifydb::catalog::Table {
                    name: TableName::new("users"),
                    columns: Columns::new([
                        Column {
                            name: ColumnName::new("id"),
                            value_type: ValueType::Int2,
                            default: None,
                        },
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
                    ]),
                }),
            }))
        } else {
            Ok(Some(Store {
                name: StoreName::new("other_users"),
                kind: StoreKind::Table(reifydb::catalog::Table {
                    name: TableName::new("other_users"),
                    columns: Columns::new([
                        Column {
                            name: ColumnName::new("id"),
                            value_type: ValueType::Int2,
                            default: None,
                        },
                        Column {
                            name: ColumnName::new("name"),
                            value_type: ValueType::Text,
                            default: None,
                        },
                    ]),
                }),
            }))
        }
    }

    fn list(&self) -> reifydb::catalog::Result<Vec<Store>> {
        todo!()
    }
}

fn main() {
    let catalog = DummyCatalog {};

    let mut db = Database { catalog, tables: HashMap::new() };

    db.tables.insert(
        "users".to_string(),
        Table {
            rows: vec![
                vec![Value::Int2(1), Value::Text("Alice".to_string()), Value::Boolean(true)],
                vec![Value::Int2(2), Value::Text("Bob".to_string()), Value::Boolean(false)],
            ],
        },
    );

    db.tables.insert(
        "other_users".to_string(),
        Table {
            rows: vec![
                vec![Value::Int2(3), Value::Text("OtherAlice".to_string())],
                vec![Value::Int2(4), Value::Text("OtherBob".to_string())],
            ],
        },
    );

    let statements = ast::parse(
        r#"
        FROM other_users
        LIMIT 2
        SELECT id, name, gender, name, id
    "#,
    );

    for statement in statements {
        let plan = plan(statement).unwrap();

        let result = execute_plan(&plan, &db).unwrap();
        for row in result {
            println!("{:?}", row);
        }
    }
}
