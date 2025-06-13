// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::transaction::serializable::{Serializable, TransactionRx, TransactionTx};
use crate::{
    Commit, Rollback, Rx, SchemaGet, SchemaInsert, SchemaScan, TableInsert, TableInsertInto,
    TableRowGet, TableRowScan, Transaction, Tx,
};
use reifydb_core::catalog::{SchemaId, TableId};
use reifydb_core::hook::Hooks;
use reifydb_core::row::{EncodedRow, EncodedRowIter};
use reifydb_core::{EncodedKey, Key, SchemaKey};
use reifydb_storage::Storage;

impl<S: Storage> Transaction<S> for Serializable<S> {
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

impl<S: Storage> TableRowGet for TransactionRx<S> {
    fn get_table_row(
        &self,
        table_id: TableId,
        keys: &[EncodedKey],
    ) -> crate::Result<Vec<EncodedRow>> {
        todo!()
    }
}

impl<S: Storage> TableRowScan for TransactionRx<S> {
    fn scan_table_row(&mut self, table_id: TableId) -> crate::Result<EncodedRowIter> {
        todo!()
    }
}

impl<S: Storage> SchemaGet for TransactionRx<S> {
    fn get_schema(&self, schema_id: SchemaId) -> crate::Result<Vec<EncodedRow>> {
        todo!()
    }
}

impl<S: Storage> SchemaScan for TransactionRx<S> {
    fn scan_schema(&mut self) -> crate::Result<EncodedRowIter> {
        todo!()
    }
}

impl<S: Storage> Rx for TransactionRx<S> {}

impl<S: Storage> TableRowGet for TransactionTx<S> {
    fn get_table_row(
        &self,
        table_id: TableId,
        keys: &[EncodedKey],
    ) -> crate::Result<Vec<EncodedRow>> {
        todo!()
    }
}

impl<S: Storage> TableRowScan for TransactionTx<S> {
    fn scan_table_row(&mut self, table_id: TableId) -> crate::Result<EncodedRowIter> {
        todo!()
    }
}

impl<S: Storage> SchemaGet for TransactionTx<S> {
    fn get_schema(&self, schema_id: SchemaId) -> crate::Result<Vec<EncodedRow>> {
        todo!()
    }
}

impl<S: Storage> SchemaScan for TransactionTx<S> {
    fn scan_schema(&mut self) -> crate::Result<EncodedRowIter> {
        todo!()
    }
}

impl<S: Storage> Rx for TransactionTx<S> {}

impl<S: Storage> Commit for TransactionTx<S> {
    fn commit(mut self) -> crate::Result<()> {
        TransactionTx::commit(&mut self)?;
        Ok(())
    }
}

impl<S: Storage> Rollback for TransactionTx<S> {
    fn rollback(mut self) -> crate::Result<()> {
        TransactionTx::rollback(&mut self)?;
        Ok(())
    }
}

impl<S: Storage> SchemaInsert for TransactionTx<S> {
    fn insert_schema(&mut self, schema_id: SchemaId, row: EncodedRow) -> crate::Result<()> {
        self.set(Key::Schema(SchemaKey { schema_id }).encode(), row)?;
        Ok(())
    }
}

impl<S: Storage> TableInsert for TransactionTx<S> {
    fn insert_table(&mut self, table_id: TableId, row: EncodedRow) -> crate::Result<()> {
        todo!()
    }
}

impl<S: Storage> TableInsertInto for TransactionTx<S> {
    fn insert_into_table(&mut self, table_id: TableId, rows: Vec<EncodedRow>) -> crate::Result<()> {
        todo!()
    }
}

impl<S: Storage> Tx for TransactionTx<S> {}
