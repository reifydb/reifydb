// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::row::RowId;
use crate::sequence::generator::u64::GeneratorU64;
use reifydb_core::interface::{
    ActiveWriteTransaction, EncodableKey, TableId, TableRowSequenceKey, UnversionedTransaction,
    VersionedTransaction,
};

pub struct TableRowSequence {}

impl TableRowSequence {
    pub fn next_row_id<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        table: TableId,
    ) -> crate::Result<RowId> {
        GeneratorU64::next(atx, &TableRowSequenceKey { table }.encode()).map(RowId)
    }
}
