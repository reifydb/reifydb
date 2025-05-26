// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use r#impl::Schema;

mod r#impl;

use crate::catalog::store::{StoreRx, StoreTx};
use base::ValueKind;
use base::expression::Expression;

pub trait SchemaRx {
    type StoreRx: StoreRx;
    // returns most recent version
    fn get(&self, store: &str) -> crate::Result<&Self::StoreRx>;

    // returns the store as of the specified version
    // fn get_as_of(&self, name: &str, version) -> Result<Option<Store>>;

    fn list(&self) -> crate::Result<Vec<&Self::StoreRx>>;
}

#[derive(Debug)]
pub struct ColumnToCreate {
    pub name: String,
    pub value: ValueKind,
    pub default: Option<Expression>,
}

pub enum StoreToCreate {
    Series { name: String, columns: Vec<ColumnToCreate> },
    Table { name: String, columns: Vec<ColumnToCreate> },
}

pub trait SchemaTx: SchemaRx {
    type StoreTx: StoreTx;

    fn create(&mut self, store: StoreToCreate) -> crate::Result<()>;

    fn create_if_not_exists(&mut self, store: StoreToCreate) -> crate::Result<()>;

    fn drop(&mut self, name: &str) -> crate::Result<()>;
}
