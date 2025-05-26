// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::catalog::{Catalog, Schema};
use crate::transaction::svl::SvlInner;
use crate::transaction::svl::lock::{ReadGuard, WriteGuard};
use crate::{CatalogRx as _, CatalogTx, InsertResult};
use base::encoding::{Value as OtherValue, bincode};
use base::{Key, Row, RowIter, key_prefix};
use persistence::Persistence;
use std::cell::RefCell;
use std::collections::HashMap;

pub struct Transaction<P: Persistence> {
    engine: ReadGuard<SvlInner<P>>,
}

impl<P: Persistence> Transaction<P> {
    pub fn new(engine: ReadGuard<SvlInner<P>>) -> Self {
        Self { engine }
    }
}

impl<P: Persistence> crate::Rx for Transaction<P> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        Ok(&self.engine.catalog)
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.engine.catalog.get(schema).unwrap())
    }

    fn get(&self, store: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        unreachable!()
    }

    fn scan(&self, store: &str) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.engine
                .persistence
                .scan_prefix(key_prefix!("{}::row::", store))
                .map(|r| Row::decode(&r.unwrap().1).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

pub struct TransactionMut<P: Persistence> {
    svl: WriteGuard<SvlInner<P>>,
    log: RefCell<HashMap<String, Vec<Row>>>,
}

impl<P: Persistence> TransactionMut<P> {
    pub fn new(svl: WriteGuard<SvlInner<P>>) -> Self {
        Self { svl, log: RefCell::new(HashMap::new()) }
    }
}

impl<P: Persistence> crate::Rx for TransactionMut<P> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        Ok(&self.svl.catalog)
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.svl.catalog.get(schema).unwrap())
    }

    fn get(&self, store: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan(&self, store: &str) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.svl
                .persistence
                .scan_prefix(key_prefix!("{}::row::", store))
                .map(|r| Row::decode(&r.unwrap().1).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

impl<P: Persistence> crate::Tx for TransactionMut<P> {
    type CatalogMut = Catalog;
    type SchemaMut = Schema;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogMut> {
        Ok(&mut self.svl.catalog)
    }

    fn schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaMut> {
        // fixme has schema?!
        // Ok()
        let schema = self.svl.catalog.get_mut(schema).unwrap();

        Ok(schema)
    }

    fn insert(&mut self, store: &str, rows: Vec<Row>) -> crate::Result<InsertResult> {
        let inserted = rows.len();
        self.log.borrow_mut().insert(store.to_string(), rows);
        Ok(InsertResult { inserted })
    }

    fn commit(mut self) -> crate::Result<()> {
        let log = self.log.borrow_mut();

        for (store, rows) in log.iter() {
            // FIXME store this information in KV

            let last_id =
                self.svl.persistence.scan_prefix(&key_prefix!("{}::row::", store)).count();

            for (id, row) in rows.iter().enumerate() {
                self.svl
                    .persistence
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
