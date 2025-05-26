// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use catalog::{
    CatalogRx, CatalogTx, ColumnToCreate, NopStore, SchemaRx, SchemaTx, StoreRx, StoreToCreate,
    StoreTx,
};
pub use error::Error;
pub use transaction::{InsertResult, Rx, Transaction, Tx, mvcc, svl};

mod catalog;
mod error;
mod transaction;

pub type Result<T> = std::result::Result<T, Error>;
