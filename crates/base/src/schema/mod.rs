// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use column::*;
pub use error::Error;
use std::ops::Deref;
pub use table::*;

mod column;
mod error;
mod table;

#[derive(Debug)]
pub struct StoreName(String);

impl Deref for StoreName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

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
    // Log
    // Ring
    // Series
    // Stack
    Table(Table),
}

#[derive(Debug)]
pub struct Store {
    pub name: StoreName,
    pub kind: StoreKind,
}

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct SchemaName(String);

impl Deref for SchemaName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl SchemaName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl From<&str> for SchemaName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub name: SchemaName,
}
