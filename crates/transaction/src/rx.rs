// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::EncodedKey;
use reifydb_core::catalog::{SchemaId, TableId};
use reifydb_core::row::{EncodedRow, EncodedRowIter};

pub trait Rx: SchemaGet + SchemaScan + TableRowGet + TableRowScan {}

pub trait SchemaGet {
    fn get_schema(&self, schema_id: SchemaId) -> crate::Result<Vec<EncodedRow>>;
}

pub trait SchemaScan {
    fn scan_schema(&mut self) -> crate::Result<EncodedRowIter>;
}

pub trait TableRowGet {
    fn get_table_row(
        &self,
        table_id: TableId,
        keys: &[EncodedKey],
    ) -> crate::Result<Vec<EncodedRow>>;
}

pub trait TableRowScan {
    fn scan_table_row(&mut self, table_id: TableId) -> crate::Result<EncodedRowIter>;
}
