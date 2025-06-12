// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::transaction::optimistic::{Optimistic, TransactionRx, TransactionTx};
use crate::{CATALOG, InsertResult, Transaction};
use reifydb_catalog::{Catalog, CatalogRx, CatalogTx, Schema};
use reifydb_core::hook::Hooks;
use reifydb_core::row::{EncodedRow, EncodedRowIter};
use reifydb_core::{EncodedKey, Key, TableRowKey, Value};
use reifydb_storage::Storage;

/// Optimistic Concurrency Control
impl<S: Storage> Transaction<S> for Optimistic<S> {
    type Rx = TransactionRx<S>;
    type Tx = TransactionTx<S>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        Ok(self.begin_read_only())
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        Ok(self.begin())
    }

    fn hooks(&self) -> Hooks {
        self.hooks.clone()
    }

    fn storage(&self) -> S {
        self.storage.clone()
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

    fn get(&self, store: &str, ids: &[EncodedKey]) -> crate::Result<Vec<EncodedRow>> {
        todo!()
    }

    fn scan_table(&mut self, schema: &str, store: &str) -> crate::Result<EncodedRowIter> {
        Ok(Box::new(
            self.scan_range(TableRowKey::full_scan(1))
                .map(|stored| stored.row)
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

    fn get(&self, store: &str, ids: &[EncodedKey]) -> crate::Result<Vec<EncodedRow>> {
        todo!()
    }

    fn scan_table(&mut self, schema: &str, store: &str) -> crate::Result<EncodedRowIter> {
        Ok(Box::new(
            self.scan_range(TableRowKey::full_scan(1))
                .unwrap()
                .map(|r| r.row().clone())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

impl<S: Storage> crate::Tx for TransactionTx<S> {
    type CatalogTx = Catalog;
    type SchemaTx = Schema;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogTx> {
        // FIXME replace this
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaTx> {
        let schema = self.catalog_mut().unwrap().get_mut(schema).unwrap();

        Ok(schema)
    }

    fn insert_into_table(
        &mut self,
        schema: &str,
        table: &str,
        rows: Vec<EncodedRow>,
    ) -> crate::Result<InsertResult> {
        let last_id = self.scan_range(TableRowKey::full_scan(1)).unwrap().count();

        // FIXME assumes every row gets inserted - not updated etc..
        let inserted = rows.len();

        for (id, row) in rows.into_iter().enumerate() {
            self.set(
                Key::TableRow(TableRowKey { table_id: 1, row_id: (last_id + id + 1) as u64 })
                    .encode(),
                row,
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
        // let last_id = self
        //     .scan_range(EncodedKeyRange::prefix(&key_prefix!("{}::{}::row::", schema, series)))
        //     .unwrap()
        //     .count();
        //
        // // FIXME assumes every row gets inserted - not updated etc..
        // let inserted = rows.len();
        //
        // for (id, row) in rows.iter().enumerate() {
        //     self.set(
        //         key_prefix!("{}::{}::row::{}", schema, series, (last_id + id + 1)).clone(),
        //         EncodedRow(AsyncCowVec::new(bincode::serialize(row))),
        //     )
        //     .unwrap();
        // }
        // // let mut persistence = self.persistence.lock().unwrap();
        // // let inserted = persistence.table_append_rows(schema, table, &rows).unwrap();
        // Ok(InsertResult { inserted })
        unimplemented!()
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
