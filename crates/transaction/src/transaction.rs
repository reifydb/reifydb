// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{CatalogRx, CatalogTx, SchemaRx, SchemaTx};
use base::expression::Expression;
use base::{Key, Row, RowIter};

/// A Rx executes transactional read operations on stores.
pub trait Rx {
    type Catalog: CatalogRx;
    type Schema: SchemaRx;

    fn catalog(&self) -> crate::Result<&Self::Catalog>;

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema>;

    /// Fetches store rows by primary key, if they exist.
    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>>;

    /// Scans a store's rows, optionally applying the given filter.
    fn scan(&self, store: &str, filter: Option<Expression>) -> crate::Result<RowIter>;
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

    fn insert(&mut self, store: impl AsRef<str>, rows: Vec<Row>) -> crate::Result<InsertResult>;

    /// Commits the transaction.
    fn commit(self) -> crate::Result<()>;
    /// Rolls back the transaction.
    fn rollback(self) -> crate::Result<()>;
}
