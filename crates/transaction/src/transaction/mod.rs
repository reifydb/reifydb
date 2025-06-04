// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_catalog::{CatalogRx, CatalogTx, SchemaRx, SchemaTx};
use reifydb_core::{Row, RowIter, Value};
use reifydb_storage::{Key, Storage};

mod optimistic;
mod serializable;

pub trait Transaction<S: Storage>: Send + Sync {
    type Rx: Rx;
    type Tx: Tx;

    /// Begins a read-only transaction.
    fn begin_read_only(&self) -> crate::Result<Self::Rx>;

    /// Begins a read-write transaction.
    fn begin(&self) -> crate::Result<Self::Tx>;
}

/// A Rx executes transactional read operations on stores.
pub trait Rx {
    type Catalog: CatalogRx;
    type Schema: SchemaRx;

    fn catalog(&self) -> crate::Result<&Self::Catalog>;

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema>;

    /// Fetches store rows by primary key, if they exist.
    fn get(&self, store: &str, ids: &[Key]) -> crate::Result<Vec<Row>>;

    /// Scans all store's rows
    fn scan_table(&mut self, schema: &str, store: &str) -> crate::Result<RowIter>;
}

#[derive(Debug)]
pub struct InsertResult {
    pub inserted: usize,
}

/// A Tx executes transactional read & write operations on stores.
pub trait Tx: Rx {
    type CatalogTx: CatalogTx;
    type SchemaTx: SchemaTx;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogTx>;

    fn schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaTx>;

    fn insert_into_table(
        &mut self,
        schema: &str,
        table: &str,
        rows: Vec<Row>,
    ) -> crate::Result<InsertResult>;

    fn insert_into_series(
        &mut self,
        schema: &str,
        series: &str,
        rows: Vec<Vec<Value>>,
    ) -> crate::Result<InsertResult>;

    /// Commits the transaction.
    fn commit(self) -> crate::Result<()>;
    /// Rolls back the transaction.
    fn rollback(self) -> crate::Result<()>;
}
