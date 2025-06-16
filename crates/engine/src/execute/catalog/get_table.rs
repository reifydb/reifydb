// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::execute::catalog::layout::table;
use reifydb_core::TableKey;
use reifydb_core::catalog::{SchemaId, Table, TableId};
use reifydb_core::row::EncodedRow;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn get_table_by_name(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        name: &str,
    ) -> crate::Result<Option<Table>> {
        Ok(tx.scan_range(TableKey::full_scan())?.find_map(|versioned| {
            let row: &EncodedRow = &versioned.row;
            let schema_name = table::LAYOUT.get_str(row, table::NAME);
            if name == schema_name {
                let id = TableId(table::LAYOUT.get_u32(row, table::ID));
                let schema = SchemaId(table::LAYOUT.get_u32(row, table::SCHEMA));
                Some(Table { id, schema, name: schema_name.to_string() })
            } else {
                None
            }
        }))
    }
}
