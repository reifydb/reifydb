// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::store::{Store, StoreMut};
use base::ValueType;
use base::expression::Expression;
use base::schema::{ColumnName, StoreName};

pub trait Schema {
    type Store: Store;
    // returns most recent version
    fn get(&self, store: impl AsRef<str>) -> crate::Result<&Self::Store>;

    // returns the store as of the specified version
    // fn get_as_of(&self, name: impl AsRef<str>, version) -> Result<Option<Store>>;

    fn list(&self) -> crate::Result<Vec<&Self::Store>>;
}

#[derive(Debug)]
pub struct ColumnToCreate {
    pub name: ColumnName,
    pub value: ValueType,
    pub default: Option<Expression>,
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
