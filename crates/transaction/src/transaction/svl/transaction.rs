// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::catalog::{Catalog, Schema};
use crate::transaction::svl::SvlInner;
use crate::transaction::svl::lock::{ReadGuard, WriteGuard};
use crate::{CATALOG, CatalogRx as _, CatalogTx, InsertResult};
use reifydb_core::{Key, Row, RowIter, Value};
use reifydb_storage::Storage;
use std::cell::RefCell;
use std::collections::HashMap;

pub struct TransactionRx<S: Storage> {
    engine: ReadGuard<SvlInner<S>>,
}

impl<S: Storage> TransactionRx<S> {
    pub fn new(engine: ReadGuard<SvlInner<S>>) -> Self {
        Self { engine }
    }
}

impl<S: Storage> crate::Rx for TransactionRx<S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&'static Self::Catalog> {
        // FIXME replace me
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.catalog().unwrap().get(schema).unwrap())
    }

    fn get(&self, store: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        unreachable!()
    }

    fn scan_table(&mut self, schema: &str, store: &str) -> crate::Result<RowIter> {
        // Ok(Box::new(
        //     self.engine.storage.scan_range(
        //         keycode::prefix_range(&key_prefix!("{}::{}::row::", schema, store)).into(),
        //     )
        //     .unwrap()
        //     .map(|r| Row::decode(&r.value()).unwrap())
        //     .collect::<Vec<_>>()
        //     .into_iter(),
        // ))
        unimplemented!()
    }
}

pub struct TransactionTx<S: Storage> {
    svl: WriteGuard<SvlInner<S>>,
    log: RefCell<HashMap<(String, String), Vec<Row>>>,
}

impl<S: Storage> TransactionTx<S> {
    pub fn new(svl: WriteGuard<SvlInner<S>>) -> Self {
        Self { svl, log: RefCell::new(HashMap::new()) }
    }
}

impl<S: Storage> crate::Rx for TransactionTx<S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&'static Self::Catalog> {
        // FIXME replace me
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.catalog().unwrap().get(schema).unwrap())
    }

    fn get(&self, store: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan_table(&mut self, schema: &str, store: &str) -> crate::Result<RowIter> {
        // Ok(Box::new(
        //     self.svl
        //         .storage
        //         .scan_prefix(key_prefix!("{}::{}::row::", schema, store))
        //         .map(|r| Row::decode(&r.unwrap().1).unwrap())
        //         .collect::<Vec<_>>()
        //         .into_iter(),
        // ))
        unimplemented!()
    }
}

impl<S: Storage> crate::Tx for TransactionTx<S> {
    type CatalogMut = Catalog;
    type SchemaMut = Schema;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogMut> {
        // FIXME replace this
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaMut> {
        let schema = self.catalog_mut().unwrap().get_mut(schema).unwrap();

        Ok(schema)
    }

    fn insert_into_table(
        &mut self,
        schema: &str,
        table: &str,
        rows: Vec<Row>,
    ) -> crate::Result<InsertResult> {
        let inserted = rows.len();
        self.log.borrow_mut().insert((schema.to_string(), table.to_string()), rows);
        Ok(InsertResult { inserted })
    }

    fn insert_into_series(
        &mut self,
        schema: &str,
        series: &str,
        rows: Vec<Vec<Value>>,
    ) -> crate::Result<InsertResult> {
        let inserted = rows.len();
        self.log.borrow_mut().insert((schema.to_string(), series.to_string()), rows);
        Ok(InsertResult { inserted })
    }

    fn commit(mut self) -> crate::Result<()> {
        // let log = self.log.borrow_mut();
        //
        // for ((schema, table), rows) in log.iter() {
        //     // FIXME store this information in KV
        //
        //     let last_id = self
        //         .svl
        //         .storage
        //         .scan_prefix(&key_prefix!("{}::{}::row::", schema, table))
        //         .count();
        //
        //     for (id, row) in rows.iter().enumerate() {
        //         self.svl
        //             .storage
        //             .set(
        //                 key_prefix!("{}::{}::row::{}", schema, table, (last_id + id + 1)),
        //                 AsyncCowVec::new(bincode::serialize(row)),
        //             )
        //             .unwrap();
        //     }
        // }
        unimplemented!();
        Ok(())
    }

    fn rollback(self) -> crate::Result<()> {
        todo!()
    }
}
