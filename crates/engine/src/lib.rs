// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use crate::session::Session;
use base::expression::Expression;
use base::schema::{Schema, SchemaMut, Store};
use base::{Key, Row, RowIter, schema};
pub use error::Error;

mod error;
pub mod execute;
mod mvcc;
mod session;

pub type Result<T> = std::result::Result<T, Error>;

pub trait Catalog {
    type Schema: Schema;

    fn get(&self, name: impl AsRef<str>) -> schema::Result<Option<Self::Schema>>;

    fn list(&self) -> schema::Result<Vec<Self::Schema>>;
}

pub trait CatalogMut: Catalog {
    fn create(&self, store: Store) -> schema::Result<()>;

    fn create_if_not_exists(&self, store: Store) -> schema::Result<()>;

    fn drop(&self, name: impl AsRef<str>) -> schema::Result<()>;
}

/// A Transaction executes transactional read operations on stores.
/// Provides snapshot isolation.
pub trait Transaction {
    type Catalog: Catalog;
    type Schema: Schema;

    fn catalog(&self) -> Result<Self::Catalog>;

    fn schema(&self) -> Result<Option<Self::Schema>>;

    /// Fetches store rows by primary key, if they exist.
    fn get(&self, table: &str, ids: &[Key]) -> Result<Vec<Row>>;
    /// Scans a store's rows, optionally applying the given filter.
    fn scan(&self, store: impl AsRef<str>, filter: Option<Expression>) -> Result<RowIter>;
}

/// A TransactionMut executes transactional read & write operations on stores.
/// Provides snapshot isolation.
pub trait TransactionMut: Transaction {
    type CatalogMut: CatalogMut;
    type SchemaMut: SchemaMut;

    fn catalog_mut(&self) -> Result<Self::CatalogMut>;

    fn schema_mut(&self) -> Result<Option<Self::SchemaMut>>;

    /// Commits the transaction.
    fn commit(self) -> Result<()>;
    /// Rolls back the transaction.
    fn rollback(self) -> Result<()>;
}

pub trait Engine<'a>: Sized {
    type Rx: Transaction + 'a;
    type Tx: TransactionMut + 'a;

    /// Begins a read-write transaction.
    fn begin(&'a self) -> Result<Self::Tx>;

    /// Begins a read-only transaction.
    fn begin_read_only(&'a self) -> Result<Self::Rx>;

    /// Creates a client session for executing RQL statements.
    fn session(&'a self) -> Session<'a, Self> {
        Session::new(self)
    }
}
