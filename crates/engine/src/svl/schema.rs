// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::ValueType;
use base::schema::{Column, ColumnName, Columns, Store, StoreKind, StoreName, Table, TableName};

pub struct Schema {}

impl crate::Schema for Schema {
    fn get(&self, name: impl AsRef<str>) -> crate::Result<Option<Store>> {
        let name = name.as_ref();

        if name == "users" {
            Ok(Some(Store {
                name: StoreName::new("users"),
                kind: StoreKind::Table(Table {
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
                kind: StoreKind::Table(Table {
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

    fn list(&self) -> crate::Result<Vec<Store>> {
        todo!()
    }
}

pub struct SchemaMut {}

impl crate::Schema for SchemaMut {
    fn get(&self, name: impl AsRef<str>) -> crate::Result<Option<Store>> {
        todo!()
    }

    fn list(&self) -> crate::Result<Vec<Store>> {
        todo!()
    }
}

impl crate::SchemaMut for SchemaMut {
    fn create(&self, store: Store) -> crate::Result<()> {
        todo!()
    }

    fn create_if_not_exists(&self, store: Store) -> crate::Result<()> {
        todo!()
    }

    fn drop(&self, name: impl AsRef<str>) -> crate::Result<()> {
        todo!()
    }
}
