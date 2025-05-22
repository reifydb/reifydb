// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::EngineInner;
use crate::svl::catalog::Catalog;
use crate::svl::lock::{ReadGuard, WriteGuard};
use crate::svl::schema::Schema;
use crate::{CatalogRx as _, CatalogTx, InsertResult};
use base::encoding::{Value as OtherValue, bincode};
use base::{Key, Row, RowIter, key_prefix};
use std::cell::RefCell;
use std::collections::HashMap;

pub struct Transaction<S: storage::StorageEngine> {
    engine: ReadGuard<EngineInner<S>>,
}

impl<S: storage::StorageEngine> Transaction<S> {
    pub fn new(engine: ReadGuard<EngineInner<S>>) -> Self {
        Self { engine }
    }
}

impl<S: storage::StorageEngine> crate::Rx for Transaction<S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        Ok(&self.engine.catalog)
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.engine.catalog.get(schema).unwrap())
    }

    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>> {
        unreachable!()
    }

    fn scan(&self, store: &str) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.engine
                .storage
                .scan_prefix(key_prefix!("{}::row::", store))
                .map(|r| Row::decode(&r.unwrap().1).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

pub struct TransactionMut<S: storage::StorageEngine> {
    engine: WriteGuard<EngineInner<S>>,
    log: RefCell<HashMap<String, Vec<Row>>>,
}

impl<S: storage::StorageEngine> TransactionMut<S> {
    pub fn new(engine: WriteGuard<EngineInner<S>>) -> Self {
        Self { engine, log: RefCell::new(HashMap::new()) }
    }
}

impl<S: storage::StorageEngine> crate::Rx for TransactionMut<S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        Ok(&self.engine.catalog)
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.engine.catalog.get(schema).unwrap())
    }

    fn get(&self, store: impl AsRef<str>, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan(&self, store: &str) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.engine
                .storage
                .scan_prefix(key_prefix!("{}::row::", store))
                .map(|r| Row::decode(&r.unwrap().1).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

impl<S: storage::StorageEngine> crate::Tx for TransactionMut<S> {
    type CatalogMut = Catalog;
    type SchemaMut = Schema;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogMut> {
        Ok(&mut self.engine.catalog)
    }

    fn schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaMut> {
        // fixme has schema?!
        // Ok()
        let schema = self.engine.catalog.get_mut(schema).unwrap();

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
