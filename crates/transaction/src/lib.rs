// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use crate::catalog::Catalog;
pub use catalog::{
    CatalogRx, CatalogTx, ColumnToCreate, NopStore, SchemaRx, SchemaTx, StoreRx, StoreToCreate,
    StoreTx,
};
pub use error::Error;
use reifydb_core::AsyncCowVec;
use std::cell::UnsafeCell;
use std::sync::OnceLock;
pub use transaction::{InsertResult, Rx, Transaction, Tx, old_mvcc, svl};

mod catalog;
mod error;
pub mod mvcc;
mod transaction;

pub type Key = AsyncCowVec<u8>;
pub type Value = AsyncCowVec<u8>;

pub type Result<T> = std::result::Result<T, Error>;

// FIXME remove this - just a quick hack

#[derive(Debug)]
pub struct CatalogCell(UnsafeCell<&'static mut Catalog>);

unsafe impl Sync for CatalogCell {} // ⚠️ only safe in single-threaded context

static CATALOG: OnceLock<CatalogCell> = OnceLock::new();

pub fn catalog_init() {
    let boxed = Box::new(Catalog::new());
    let leaked = Box::leak(boxed);
    CATALOG.set(CatalogCell(UnsafeCell::new(leaked))).unwrap();
}

pub fn catalog_mut_singleton() -> &'static mut Catalog {
    // SAFETY: Caller guarantees exclusive access
    unsafe { *CATALOG.get().unwrap().0.get() }
}
