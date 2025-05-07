// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use column::*;
pub use error::Error;
pub use table::*;

mod column;
mod error;
mod table;

#[derive(Debug)]
pub struct StoreName(String);

impl StoreName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl From<&str> for StoreName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug)]
pub enum StoreKind {
    Table(Table),
}

#[derive(Debug)]
pub struct Store {
    pub name: StoreName,
    pub kind: StoreKind,
}

pub type Result<T> = core::result::Result<T, Error>;

pub trait Catalog {
    fn get(&self, name: impl AsRef<str>) -> Result<Option<Store>>;

    fn list(&self) -> Result<Vec<Store>>;
}

pub trait CatalogMut {
    fn create(&self, store: Store) -> Result<()>;

    fn create_if_not_exists(&self, store: Store) -> Result<()>;

    fn drop(&self, name: impl AsRef<str>) -> Result<()>;
}
