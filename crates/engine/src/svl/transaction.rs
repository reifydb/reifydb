// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::EngineInner;
use crate::svl::catalog::{Catalog, CatalogMut};
use crate::svl::schema::{Schema, SchemaMut};
use base::encoding::{Value as OtherValue, bincode};
use base::expression::Expression;
use base::{Key, Row, RowIter};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

pub struct Transaction<'a, S: storage::EngineMut> {
    engine: RwLockReadGuard<'a, EngineInner<S>>,
}

impl<'a, S: storage::EngineMut> Transaction<'a, S> {
    pub fn new(engine: RwLockReadGuard<'a, EngineInner<S>>) -> Self {
        Self { engine }
    }
}

impl<'a, S: storage::EngineMut> crate::Transaction for Transaction<'a, S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<Self::Catalog> {
        Ok(Catalog {})
    }

    fn schema(&self) -> crate::Result<Option<Self::Schema>> {
        Ok(Some(Schema {}))
    }

    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>> {
        unreachable!()
    }

    fn scan(&self, store: impl AsRef<str>, filter: Option<Expression>) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.engine
                .storage
                .scan_prefix(&vec![])
                .map(|r| Row::decode(&r.unwrap().1).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

pub struct TransactionMut<'a, S: storage::EngineMut> {
    engine: RwLockWriteGuard<'a, EngineInner<S>>,
    log: RefCell<HashMap<String, Vec<Row>>>,
}

impl<'a, S: storage::EngineMut> TransactionMut<'a, S> {
    pub fn new(engine: RwLockWriteGuard<'a, EngineInner<S>>) -> Self {
        Self { engine, log: RefCell::new(HashMap::new()) }
    }
}

impl<'a, S: storage::EngineMut> crate::Transaction for TransactionMut<'a, S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<Self::Catalog> {
        todo!()
    }

    fn schema(&self) -> crate::Result<Option<Self::Schema>> {
        todo!()
    }

    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan(&self, store: impl AsRef<str>, filter: Option<Expression>) -> crate::Result<RowIter> {
        todo!()
    }
}

impl<'a, S: storage::EngineMut> crate::TransactionMut for TransactionMut<'a, S> {
    type CatalogMut = CatalogMut;
    type SchemaMut = SchemaMut;

    fn catalog_mut(&self) -> crate::Result<Self::CatalogMut> {
        todo!()
    }

    fn schema_mut(&self) -> crate::Result<Option<Self::SchemaMut>> {
        todo!()
    }

    fn set(&self, store: impl AsRef<str>, rows: Vec<Row>) -> crate::Result<()> {
        let store = store.as_ref();
        self.log.borrow_mut().insert(store.to_string(), rows);
        Ok(())
    }

    fn commit(mut self) -> crate::Result<()> {
        let log = self.log.borrow_mut();

        for (store, rows) in log.iter() {
            for (id, row) in rows.iter().enumerate() {
                self.engine
                    .storage
                    .set(&bincode::serialize(&(id as i64)), bincode::serialize(row))
                    .unwrap();
            }
        }

        Ok(())
    }

    fn rollback(self) -> crate::Result<()> {
        todo!()
    }
}
