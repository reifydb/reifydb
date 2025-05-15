// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::expression::Expression;
use base::schema::{SchemaName, StoreName};
use base::{Catalog, CatalogMut, Key, Row, RowIter, Schema, SchemaMut};

pub trait Engine<'a, S: storage::Engine>: Sized {
    type Rx: Transaction;
    type Tx: TransactionMut;

    /// Begins a read-write transaction.
    fn begin(&'a self) -> crate::Result<Self::Tx>;

    /// Begins a read-only transaction.
    fn begin_read_only(&'a self) -> crate::Result<Self::Rx>;
}

/// A Transaction executes transactional read operations on stores.
/// Provides snapshot isolation.
pub trait Transaction {
    type Catalog: Catalog;
    type Schema: Schema;

    fn catalog(&self) -> crate::Result<&Self::Catalog>;

    fn schema(&self, schema: impl AsRef<str>) -> crate::Result<&Self::Schema>;

    /// Fetches store rows by primary key, if they exist.
    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>>;

    /// Scans a store's rows, optionally applying the given filter.
    fn scan(
        &self,
        store: impl AsRef<StoreName>,
        filter: Option<Expression>,
    ) -> crate::Result<RowIter>;
}

#[derive(Debug)]
pub struct InsertResult {
    pub inserted: usize,
}

/// A TransactionMut executes transactional read & write operations on stores.
/// Provides snapshot isolation.
pub trait TransactionMut: Transaction {
    type CatalogMut: CatalogMut;
    type SchemaMut: SchemaMut;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogMut>;

    fn schema_mut(&mut self, schema: impl AsRef<SchemaName>)
    -> crate::Result<&mut Self::SchemaMut>;

    fn insert(&mut self, store: impl AsRef<str>, rows: Vec<Row>) -> crate::Result<InsertResult>;

    /// Commits the transaction.
    fn commit(self) -> crate::Result<()>;
    /// Rolls back the transaction.
    fn rollback(self) -> crate::Result<()>;
}
