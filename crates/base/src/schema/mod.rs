// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use column::*;
pub use error::Error;
use std::ops::Deref;

mod column;
mod error;

#[derive(Debug, Clone)]
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

impl AsRef<StoreName> for StoreName {
    fn as_ref(&self) -> &StoreName {
        &self
    }
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

impl AsRef<SchemaName> for SchemaName {
    fn as_ref(&self) -> &SchemaName {
        &self
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

impl Into<String> for SchemaName {
    fn into(self) -> String {
        self.0
    }
}
