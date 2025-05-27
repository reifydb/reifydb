// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{CatalogRx, CatalogTx, SchemaRx, SchemaTx};
use base::{Key, Row, RowIter};

pub mod mvcc;
pub mod svl;

pub trait Transaction<P: persistence::Persistence>: Send + Sync {
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
    fn scan_table(&self, schema: &str, store: &str) -> crate::Result<RowIter>;
}

#[derive(Debug)]
pub struct InsertResult {
    pub inserted: usize,
}

/// A Tx executes transactional read & write operations on stores.
pub trait Tx: Rx {
    type CatalogMut: CatalogTx;
    type SchemaMut: SchemaTx;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogMut>;

    fn schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaMut>;

    fn insert_into_table(
        &mut self,
        schema: &str,
        table: &str,
        rows: Vec<Row>,
    ) -> crate::Result<InsertResult>;

    /// Commits the transaction.
    fn commit(self) -> crate::Result<()>;
    /// Rolls back the transaction.
    fn rollback(self) -> crate::Result<()>;
}
