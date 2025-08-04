// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sequence::u64::SequenceGeneratorU64;
use reifydb_core::interface::{
    ActiveWriteTransaction, ColumnId, EncodableKey, TableColumnSequenceKey, TableId,
    UnversionedTransaction, VersionedTransaction,
};

pub struct ColumnSequence {}

impl ColumnSequence {
    pub fn next_value<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        table: TableId,
        column: ColumnId,
    ) -> crate::Result<u64> {
        SequenceGeneratorU64::next(atx, &TableColumnSequenceKey { table, column }.encode())
    }
}