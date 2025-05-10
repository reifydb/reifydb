// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::engine::execute::execute_plan;
use reifydb::engine::{Catalog, CatalogMut, Engine, Transaction, TransactionMut};
use reifydb::expression::Expression;
use reifydb::rql::ast;
use reifydb::rql::plan::plan;
use reifydb::schema::{
    Column, ColumnName, Columns, Schema, SchemaMut, Store, StoreKind, StoreName, TableName,
};
use reifydb::{Key, Row, RowIter, Value, ValueType, engine};

struct DummyCatalog {}

impl Catalog for DummyCatalog {
    type Schema = DummySchema;

    fn get(&self, name: impl AsRef<str>) -> reifydb::schema::Result<Option<Self::Schema>> {
        Ok(Some(DummySchema {}))
    }

    fn list(&self) -> reifydb::schema::Result<Vec<Self::Schema>> {
        todo!()
    }
}

struct DummyCatalogMut {}

impl Catalog for DummyCatalogMut {
    type Schema = DummySchema;

    fn get(&self, name: impl AsRef<str>) -> reifydb::schema::Result<Option<Self::Schema>> {
        todo!()
    }

    fn list(&self) -> reifydb::schema::Result<Vec<Self::Schema>> {
        todo!()
    }
}

impl CatalogMut for DummyCatalogMut {
    fn create(&self, store: Store) -> reifydb::schema::Result<()> {
        todo!()
    }

    fn create_if_not_exists(&self, store: Store) -> reifydb::schema::Result<()> {
        todo!()
    }

    fn drop(&self, name: impl AsRef<str>) -> reifydb::schema::Result<()> {
        todo!()
    }
}

struct DummySchema {}

impl Schema for DummySchema {
    fn get(&self, name: impl AsRef<str>) -> reifydb::schema::Result<Option<Store>> {
        let name = name.as_ref();

        if name == "users" {
            Ok(Some(Store {
                name: StoreName::new("users"),
                kind: StoreKind::Table(reifydb::schema::Table {
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
                kind: StoreKind::Table(reifydb::schema::Table {
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

    fn list(&self) -> reifydb::schema::Result<Vec<Store>> {
        todo!()
    }
}

struct DummySchemaMut {}

impl Schema for DummySchemaMut {
    fn get(&self, name: impl AsRef<str>) -> reifydb::schema::Result<Option<Store>> {
        todo!()
    }

    fn list(&self) -> reifydb::schema::Result<Vec<Store>> {
        todo!()
    }
}

impl SchemaMut for DummySchemaMut {
    fn create(&self, store: Store) -> reifydb::schema::Result<()> {
        todo!()
    }

    fn create_if_not_exists(&self, store: Store) -> reifydb::schema::Result<()> {
        todo!()
    }

    fn drop(&self, name: impl AsRef<str>) -> reifydb::schema::Result<()> {
        todo!()
    }
}

struct DummyTransaction {}

impl Transaction for DummyTransaction {
    type Catalog = DummyCatalog;
    type Schema = DummySchema;

    fn catalog(&self) -> engine::Result<Self::Catalog> {
        Ok(DummyCatalog {})
    }

    fn schema(&self) -> engine::Result<Option<Self::Schema>> {
        Ok(Some(DummySchema {}))
    }

    fn get(&self, table: &str, ids: &[Key]) -> engine::Result<Vec<Row>> {
        unreachable!()
    }

    fn scan(
        &self,
        store: impl AsRef<str>,
        filter: Option<Expression>,
    ) -> reifydb::engine::Result<RowIter> {
        Ok(Box::new(
            vec![
                vec![Value::Int2(1), Value::Text("Alice".to_string()), Value::Boolean(true)],
                vec![Value::Int2(2), Value::Text("Bob".to_string()), Value::Boolean(false)],
            ]
            .into_iter(),
        ))
    }
}

struct DummyTransactionMut {}

impl Transaction for DummyTransactionMut {
    type Catalog = DummyCatalog;
    type Schema = DummySchema;

    fn catalog(&self) -> engine::Result<Self::Catalog> {
        todo!()
    }

    fn schema(&self) -> engine::Result<Option<Self::Schema>> {
        todo!()
    }

    fn get(&self, table: &str, ids: &[Key]) -> engine::Result<Vec<Row>> {
        todo!()
    }

    fn scan(&self, store: impl AsRef<str>, filter: Option<Expression>) -> engine::Result<RowIter> {
        todo!()
    }
}

impl TransactionMut for DummyTransactionMut {
    type CatalogMut = DummyCatalogMut;
    type SchemaMut = DummySchemaMut;

    fn catalog_mut(&self) -> engine::Result<Self::CatalogMut> {
        todo!()
    }

    fn schema_mut(&self) -> engine::Result<Option<Self::SchemaMut>> {
        todo!()
    }

    fn commit(self) -> engine::Result<()> {
        todo!()
    }

    fn rollback(self) -> engine::Result<()> {
        todo!()
    }
}

struct DummyEngine {}

impl<'a> Engine<'a> for DummyEngine {
    type Rx = DummyTransaction;
    type Tx = DummyTransactionMut;

    fn begin(&'a self) -> engine::Result<Self::Tx> {
        todo!()
    }

    fn begin_read_only(&'a self) -> engine::Result<Self::Rx> {
        Ok(DummyTransaction {})
    }
}

fn main() {
    let engine = DummyEngine {};

    let rx = engine.begin_read_only().unwrap();

    let statements = ast::parse(
        r#"
        FROM users
        LIMIT 2
        SELECT id, name, gender, name, id, gender
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
