// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_catalog::{CatalogRx, CatalogTx, SchemaRx, SchemaTx};
use reifydb_core::hook::Hooks;
use reifydb_core::row::{EncodedRow, EncodedRowIter};
use reifydb_core::{EncodedKey, Value};
use reifydb_storage::Storage;

mod dep_optimistic;
mod dep_serializable;
mod optimistic;
mod serializable;
mod shared;

pub trait DepTransaction<S: Storage>: Send + Sync {
    type Rx: DepRx;
    type Tx: DepTx;

    /// Begins a read-only transaction.
    fn dep_begin_read_only(&self) -> crate::Result<Self::Rx>;

    /// Begins a read-write transaction.
    fn dep_begin(&self) -> crate::Result<Self::Tx>;

    fn dep_hooks(&self) -> Hooks;

    fn dep_storage(&self) -> S;
}

/// A Rx executes transactional read operations on stores.
pub trait DepRx {
    type Catalog: CatalogRx;
    type Schema: SchemaRx;

    #[deprecated]
    fn dep_catalog(&self) -> crate::Result<&Self::Catalog>;

    #[deprecated]
    fn dep_schema(&self, schema: &str) -> crate::Result<&Self::Schema>;

    /// Fetches store rows by primary key, if they exist.
    #[deprecated]
    fn dep_get(&self, store: &str, ids: &[EncodedKey]) -> crate::Result<Vec<EncodedRow>>;

    /// Scans all store's rows
    #[deprecated]
    fn dep_scan_table(&mut self, schema: &str, store: &str) -> crate::Result<EncodedRowIter>;
}

#[derive(Debug)]
pub struct DepInsertResult {
    pub inserted: usize,
}

/// A Tx executes transactional read & write operations on stores.
pub trait DepTx: DepRx {
    type CatalogTx: CatalogTx;
    type SchemaTx: SchemaTx;

    #[deprecated]
    fn dep_catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogTx>;

    #[deprecated]
    fn dep_schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaTx>;

    #[deprecated]
    fn dep_insert_into_table(
        &mut self,
        schema: &str,
        table: &str,
        rows: Vec<EncodedRow>,
    ) -> crate::Result<DepInsertResult>;

    #[deprecated]
    fn dep_insert_into_series(
        &mut self,
        schema: &str,
        series: &str,
        rows: Vec<Vec<Value>>,
    ) -> crate::Result<DepInsertResult>;

    #[deprecated]
    /// Commits the transaction.
    fn dep_commit(self) -> crate::Result<()>;
    #[deprecated]
    /// Rolls back the transaction.
    fn dep_rollback(self) -> crate::Result<()>;
}
