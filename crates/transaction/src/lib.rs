// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use catalog::{Catalog, CatalogMut};
pub use engine::{Engine, Transaction, TransactionMut};
pub use error::Error;
pub use schema::{ColumnToCreate, Schema, SchemaMut, StoreToCreate};
pub use store::{NopStore, Store, StoreMut};

mod catalog;
mod engine;
mod error;
pub mod mvcc;
mod schema;
mod store;
pub mod svl;

pub type Result<T> = std::result::Result<T, Error>;
