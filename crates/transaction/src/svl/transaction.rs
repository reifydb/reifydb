// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::engine::InsertResult;
use crate::svl::EngineInner;
use crate::svl::catalog::Catalog;
use crate::svl::schema::Schema;
use base::encoding::{Value as OtherValue, bincode};
use base::expression::Expression;
use base::schema::{SchemaName, StoreName};
use base::{Catalog as _, CatalogMut, key_prefix};
use base::{Key, Row, RowIter};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
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

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        Ok(&self.engine.catalog)
    }

    fn schema(&self, schema: impl AsRef<str>) -> crate::Result<&Self::Schema> {
        Ok(self.engine.catalog.get(schema.as_ref()).unwrap())
    }

    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>> {
        unreachable!()
    }

    fn scan(
        &self,
        store: impl AsRef<StoreName>,
        filter: Option<Expression>,
    ) -> crate::Result<RowIter> {
        let store = store.as_ref();
        Ok(Box::new(
            self.engine
                .storage
                .scan_prefix(key_prefix!("{}::row::", store.deref()))
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

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        Ok(&self.engine.catalog)
    }

    fn schema(&self, schema: impl AsRef<str>) -> crate::Result<&Self::Schema> {
        Ok(self.engine.catalog.get(schema.as_ref()).unwrap())
    }

    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan(
        &self,
        store: impl AsRef<StoreName>,
        filter: Option<Expression>,
    ) -> crate::Result<RowIter> {
        todo!()
    }
}

impl<'a, S: storage::EngineMut> crate::TransactionMut for TransactionMut<'a, S> {
    type CatalogMut = Catalog;
    type SchemaMut = Schema;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogMut> {
        Ok(&mut self.engine.catalog)
    }

    fn schema_mut(
        &mut self,
        schema: impl AsRef<SchemaName>,
    ) -> crate::Result<&mut Self::SchemaMut> {
        // fixme has schema?!
        // Ok()
        let schema = self.engine.catalog.get_mut(schema.as_ref().deref()).unwrap();

        Ok(schema)
    }

    fn insert(&mut self, store: impl AsRef<str>, rows: Vec<Row>) -> crate::Result<InsertResult> {
        let inserted = rows.len();
        let store = store.as_ref();
        self.log.borrow_mut().insert(store.to_string(), rows);
        Ok(InsertResult { inserted })
    }

    fn commit(mut self) -> crate::Result<()> {
        let log = self.log.borrow_mut();

        for (store, rows) in log.iter() {
            // FIXME store this information in KV

            let last_id = self.engine.storage.scan_prefix(&key_prefix!("{}::row::", store)).count();

            for (id, row) in rows.iter().enumerate() {
                self.engine
                    .storage
                    .set(
                        // &encode_key(format!("{}::row::{}", store, (last_id + id + 1)).as_str()),
                        key_prefix!("{}::row::{}", store, (last_id + id + 1)),
                        bincode::serialize(row),
                    )
                    .unwrap();
            }
        }

        Ok(())
    }

    fn rollback(self) -> crate::Result<()> {
        todo!()
    }
}
