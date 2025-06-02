// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::AsyncCowVec;
use crate::catalog::{Catalog, Schema};
use crate::mvcc::transaction::optimistic::{Optimistic, TransactionRx, TransactionTx};
use crate::{CATALOG, CatalogRx, CatalogTx, InsertResult, Transaction};
use reifydb_core::encoding::{Value as _, bincode};
use reifydb_core::{Key, Row, RowIter, Value, key_prefix};
use reifydb_persistence::{KeyRange, Persistence};
use reifydb_storage::Storage;

impl<S: Storage> Transaction<S> for Optimistic<S> {
    type Rx = TransactionRx<S>;
    type Tx = TransactionTx<S>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        Ok(self.begin_read_only())
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        Ok(self.begin())
    }
}

impl<S: Storage> crate::Rx for TransactionRx<S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        // FIXME replace this
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.catalog().unwrap().get(schema).unwrap())
    }

    fn get(&self, store: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan_table(&mut self, schema: &str, store: &str) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.range(KeyRange::prefix(&key_prefix!("{}::{}::row::", schema, store)))
                .map(|r| Row::decode(&r.value).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

impl<S: Storage> crate::Rx for TransactionTx<S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        // FIXME replace this
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.catalog().unwrap().get(schema).unwrap())
    }

    fn get(&self, store: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan_table(&mut self, schema: &str, store: &str) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.scan_range(KeyRange::prefix(&key_prefix!("{}::{}::row::", schema, store)).into())
                .unwrap()
                .map(|r| Row::decode(&r.value()).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
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
        let last_id = self
            .scan_range(KeyRange::prefix(&key_prefix!("{}::{}::row::", schema, table)))
            .unwrap()
            .count();

        // FIXME assumes every row gets inserted - not updated etc..
        let inserted = rows.len();

        for (id, row) in rows.iter().enumerate() {
            self.set(
                key_prefix!("{}::{}::row::{}", schema, table, (last_id + id + 1)).clone(),
                AsyncCowVec::new(bincode::serialize(row)),
            )
            .unwrap();
        }
        // let mut persistence = self.persistence.lock().unwrap();
        // let inserted = persistence.table_append_rows(schema, table, &rows).unwrap();
        Ok(InsertResult { inserted })
    }

    fn insert_into_series(
        &mut self,
        schema: &str,
        series: &str,
        rows: Vec<Vec<Value>>,
    ) -> crate::Result<InsertResult> {
        let last_id = self
            .scan_range(KeyRange::prefix(&key_prefix!("{}::{}::row::", schema, series)))
            .unwrap()
            .count();

        // FIXME assumes every row gets inserted - not updated etc..
        let inserted = rows.len();

        for (id, row) in rows.iter().enumerate() {
            self.set(
                key_prefix!("{}::{}::row::{}", schema, series, (last_id + id + 1)).clone(),
                AsyncCowVec::new(bincode::serialize(row)),
            )
            .unwrap();
        }
        // let mut persistence = self.persistence.lock().unwrap();
        // let inserted = persistence.table_append_rows(schema, table, &rows).unwrap();
        Ok(InsertResult { inserted })
    }

    fn commit(mut self) -> crate::Result<()> {
        TransactionTx::commit(&mut self)?;
        Ok(())
    }

    fn rollback(mut self) -> crate::Result<()> {
        TransactionTx::rollback(&mut self);

        Ok(())
    }
}
