// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::session::Session;
use base::expression::Expression;
use base::schema::Store;
use base::{Key, Row, RowIter};

pub trait Engine<'a>: Sized {
    type Rx: Transaction + 'a;
    type Tx: TransactionMut + 'a;

    /// Begins a read-write transaction.
    fn begin(&'a self) -> crate::Result<Self::Tx>;

    /// Begins a read-only transaction.
    fn begin_read_only(&'a self) -> crate::Result<Self::Rx>;

    /// Creates a client session for executing RQL statements.
    fn session(&'a self) -> Session<'a, Self> {
        Session::new(self)
    }
}

/// A Transaction executes transactional read operations on stores.
/// Provides snapshot isolation.
pub trait Transaction {
    type Catalog: Catalog;
    type Schema: Schema;

    fn catalog(&self) -> crate::Result<Self::Catalog>;

    fn schema(&self) -> crate::Result<Option<Self::Schema>>;

    /// Fetches store rows by primary key, if they exist.
    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>>;
    /// Scans a store's rows, optionally applying the given filter.
    fn scan(&self, store: impl AsRef<str>, filter: Option<Expression>) -> crate::Result<RowIter>;
}

/// A TransactionMut executes transactional read & write operations on stores.
/// Provides snapshot isolation.
pub trait TransactionMut: Transaction {
    type CatalogMut: CatalogMut;
    type SchemaMut: SchemaMut;

    fn catalog_mut(&self) -> crate::Result<Self::CatalogMut>;

    fn schema_mut(&self) -> crate::Result<Option<Self::SchemaMut>>;

    fn set(&self, store: impl AsRef<str>, rows: Vec<Row>) -> crate::Result<()>;

    /// Commits the transaction.
    fn commit(self) -> crate::Result<()>;
    /// Rolls back the transaction.
    fn rollback(self) -> crate::Result<()>;
}

pub trait Catalog {
    type Schema: Schema;

    fn get(&self, schema: impl AsRef<str>) -> crate::Result<Option<Self::Schema>>;

    fn list(&self) -> crate::Result<Vec<Self::Schema>>;
}

pub trait CatalogMut: Catalog {
    fn create(&self, store: Store) -> crate::Result<()>;

    fn create_if_not_exists(&self, store: Store) -> crate::Result<()>;

    fn drop(&self, name: impl AsRef<str>) -> crate::Result<()>;
}

pub trait Schema {
    // returns most recent version
    fn get(&self, store: impl AsRef<str>) -> crate::Result<Option<Store>>;

    // returns the store as of the specified version
    // fn get_as_of(&self, name: impl AsRef<str>, version) -> Result<Option<Store>>;

    fn list(&self) -> crate::Result<Vec<Store>>;
}

pub trait SchemaMut: Schema {
    fn create(&self, store: Store) -> crate::Result<()>;

    fn create_if_not_exists(&self, store: Store) -> crate::Result<()>;

    fn drop(&self, name: impl AsRef<str>) -> crate::Result<()>;
}
