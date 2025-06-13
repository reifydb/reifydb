// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Rx;
use reifydb_core::catalog::{SchemaId, TableId};
use reifydb_core::row::EncodedRow;

pub trait Tx: Rx + Commit + Rollback + SchemaInsert + TableInsert + TableInsertInto {}

pub trait SchemaInsert {
    fn insert_schema(&mut self, schema_id: SchemaId, row: EncodedRow) -> crate::Result<()>;
}

pub trait TableInsert {
    fn insert_table(&mut self, table_id: TableId, row: EncodedRow) -> crate::Result<()>;
}

pub trait TableInsertInto {
    fn insert_into_table(&mut self, table_id: TableId, rows: Vec<EncodedRow>) -> crate::Result<()>;
}

pub trait Commit {
    fn commit(self) -> crate::Result<()>;
}

pub trait Rollback {
    fn rollback(self) -> crate::Result<()>;
}
