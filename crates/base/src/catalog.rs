// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueType;
use crate::expression::Expression;
use crate::schema::{ColumnName, SchemaName, StoreName};

#[derive(Debug)]
pub struct ColumnToCreate {
    pub name: ColumnName,
    pub value: ValueType,
    pub default: Option<Expression>,
}

pub trait Catalog {
    type Schema: Schema;

    fn get(&self, schema: impl AsRef<str>) -> crate::Result<&Self::Schema>;

    fn list(&self) -> crate::Result<Vec<&Self::Schema>>;
}

pub trait CatalogMut: Catalog {
    type SchemaMut: SchemaMut;

    fn get_mut(&mut self, schema: impl AsRef<str>) -> crate::Result<&mut Self::Schema>;

    fn create(&mut self, schema: impl AsRef<SchemaName>) -> crate::Result<()>;

    fn create_if_not_exists(&mut self, schema: impl AsRef<SchemaName>) -> crate::Result<()>;

    fn drop(&mut self, name: impl AsRef<str>) -> crate::Result<()>;
}

pub trait Schema {
    type Store: Store;
    // returns most recent version
    fn get(&self, store: impl AsRef<str>) -> crate::Result<&Self::Store>;

    // returns the store as of the specified version
    // fn get_as_of(&self, name: impl AsRef<str>, version) -> Result<Option<Store>>;

    fn list(&self) -> crate::Result<Vec<&Self::Store>>;
}

pub enum StoreToCreate {
    Table { name: StoreName, columns: Vec<ColumnToCreate> },
}

pub trait SchemaMut: Schema {
    type StoreMut: StoreMut;

    fn create(&mut self, store: StoreToCreate) -> crate::Result<()>;

    fn create_if_not_exists(&mut self, store: StoreToCreate) -> crate::Result<()>;

    fn drop(&mut self, name: impl AsRef<str>) -> crate::Result<()>;
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: ColumnName,
    pub value: ValueType,
    pub default: Option<Expression>,
}

pub trait Store {
    fn get_column(&self, column: impl AsRef<str>) -> crate::Result<Column>;

    fn list_columns(&self) -> crate::Result<Vec<Column>>;

    fn get_column_index(&self, column: impl AsRef<str>) -> crate::Result<usize>;
}

pub trait StoreMut: Store {}

pub struct NopStore {}

impl Store for NopStore {
    fn get_column(&self, _column: impl AsRef<str>) -> crate::Result<Column> {
        unreachable!()
    }

    fn list_columns(&self) -> crate::Result<Vec<Column>> {
        unreachable!()
    }

    fn get_column_index(&self, _column: impl AsRef<str>) -> crate::Result<usize> {
        unreachable!()
    }
}
