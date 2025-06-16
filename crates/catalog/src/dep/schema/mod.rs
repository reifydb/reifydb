// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use r#impl::DepSchema;

mod r#impl;

use crate::{DepColumnPolicy, DepStoreRx, DepStoreTx};
use reifydb_core::ValueKind;

pub trait DepSchemaRx {
    type StoreRx: DepStoreRx;
    // returns most recent version
    fn get(&self, store: &str) -> crate::Result<&Self::StoreRx>;

    // returns the store as of the specified version
    // fn get_as_of(&self, name: &str, version) -> Result<Option<Store>>;

    fn list(&self) -> crate::Result<Vec<&Self::StoreRx>>;
}

#[derive(Debug, Clone)]
pub struct DepColumnToCreate {
    pub name: String,
    pub value: ValueKind,
    pub policies: Vec<DepColumnPolicy>,
}

pub enum DepStoreToCreate {
    DeferredView { view: String, columns: Vec<DepColumnToCreate> },
    Series { series: String, columns: Vec<DepColumnToCreate> },
    Table { table: String, columns: Vec<DepColumnToCreate> },
}

pub trait DepSchemaTx: DepSchemaRx {
    type StoreTx: DepStoreTx;

    fn create(&mut self, store: DepStoreToCreate) -> crate::Result<()>;

    fn create_if_not_exists(&mut self, store: DepStoreToCreate) -> crate::Result<()>;

    fn drop(&mut self, name: &str) -> crate::Result<()>;
}
