// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::key::{EncodableKey, TableRowSequenceKey};
use crate::sequence::u64::SequenceGeneratorU64;
use crate::table::TableId;
use reifydb_core::catalog::RowId;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

pub struct TableRowSequence {}

impl TableRowSequence {
    pub fn next_row_id<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        table: TableId,
    ) -> crate::Result<RowId> {
        SequenceGeneratorU64::next(tx, &TableRowSequenceKey { table }.encode()).map(RowId)
    }
}
